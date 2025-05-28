import * as cheerio from "cheerio";
import {
	SQSClient,
	ReceiveMessageCommand,
	DeleteMessageCommand,
	SendMessageBatchCommand,
	GetQueueUrlCommand,
	CreateQueueCommand,
	GetQueueAttributesCommand,
	DeleteQueueCommand,
} from "@aws-sdk/client-sqs";
import Redis from "ioredis";

const sqsClient = new SQSClient({ region: "eu-west-1" });
const redis = new Redis({
	host: process.env.REDIS_HOST,
	port: Number(process.env.REDIS_PORT),
});
const EC2_REPORT_URL = process.env.EC2_REPORT_URL;

export async function handler(event) {
	console.log("Lambda invoked with event:", JSON.stringify(event));

	if (!event.Records?.length) {
		console.log("No records, exiting.");
		return;
	}

	const { eventSourceARN } = event.Records[0];
	const {CURRENT_QUEUE_URL, nextQueueUrl} = getQueueUrlFromArn(eventSourceARN);
	const result = await receiveMessagePayload(CURRENT_QUEUE_URL);

	if(!result.message)
		return;

	const { message, parsedBody } = result;

	if (!message) return;

	console.log("Parsed body: ", parsedBody);

	const { targetUrl,
		currentDepth,
		pagesCrawled, 
		clientGuid,
		maxDepth, 
		maxPages, 
		searchText } = parsedBody;

	const nextQueueFullUrl = `${nextQueueUrl}/${clientGuid}-depth-${currentDepth + 1}-queue`;
	console.log("Next queue: ", nextQueueUrl);

	if (currentDepth > maxDepth || pagesCrawled >= maxPages) {
		await deleteMessage(message, CURRENT_QUEUE_URL);
		console.log("Deleted message, going too deep");
		return;
	}

	const cacheKey = `url:${targetUrl}`;

	if (await redis.exists(cacheKey)) {
		handleCached(cacheKey, message, CURRENT_QUEUE_URL, nextQueueFullUrl, parsedBody);
		console.log("Handled cached key: ", cacheKey);
		return;
	}

	const html = await fetchHTML(targetUrl);
	if (!html) {
		await deleteMessage(message, CURRENT_QUEUE_URL);
		console.log("Not html");
		return;
	}

	const $ = cheerio.load(html);

	const nextDepthLinks = extractLinks($, targetUrl, currentDepth, maxDepth, clientGuid);

	const isMatch = isTextFound($, searchText);

	await cache(cacheKey, nextDepthLinks, isMatch);

	if(isMatch) {
		await reportToServer(clientGuid, targetUrl, currentDepth, pagesCrawled, nextDepthLinks);
		console.log("Reported back to server");
	}

	await ensureQueueExists(nextQueueFullUrl);
	await sendMessages(
		nextDepthLinks,
		parsedBody,
		nextQueueFullUrl
	);

	await deleteMessage(message, CURRENT_QUEUE_URL);

	await deleteIfQueueEmpty(CURRENT_QUEUE_URL);
}

function getQueueUrlFromArn(arn) {
	const [, , , region, accountId, queueName] = arn.split(":");
	const CURRENT_QUEUE_URL = `https://sqs.${region}.amazonaws.com/${accountId}/${queueName}`;
	const nextQueueUrl = `https://sqs.${region}.amazonaws.com/${accountId}`;

	console.log(`Current url: ${CURRENT_QUEUE_URL} \n Next url: ${nextQueueUrl}`);

	return { CURRENT_QUEUE_URL, nextQueueUrl };
}

async function receiveMessagePayload(CURRENT_QUEUE_URL) {
	const receiveMessagePayload = {
		QueueUrl: CURRENT_QUEUE_URL,
		MaxNumberOfMessages: 1,
		VisibilityTimeout: 30,
		WaitTimeSeconds: 10,
	};

	const data = await sqsClient.send(new ReceiveMessageCommand(receiveMessagePayload));

	console.log("Data from payload: ", data);

	if (!data.Messages || data.Messages.length === 0) return;

	const message = data.Messages[0];
	const parsedBody = JSON.parse(message.Body);
	return { message, parsedBody };
}

async function ensureQueueExists(queueName) {
	const QueueName = `${queueName}`;

	try {
		await sqsClient.send(new GetQueueUrlCommand({ QueueName }));
		console.log(`Queue ${QueueName} already exists`);
	} catch (error) {
		console.log(`Creating new queue: ${QueueName}`);
		await sqsClient.send(new CreateQueueCommand({
			QueueName,
			Attributes: {
				VisibilityTimeout: "30",
				MessageRetentionPeriod: "1200",
			}
		}));
	}
}

async function deleteMessage(message, queueUrl) {
	if (!message.ReceiptHandle) return;
	await sqsClient.send(
		new DeleteMessageCommand({
			QueueUrl: queueUrl,
			ReceiptHandle: message.ReceiptHandle,
		})
	);
}

async function handleCached(cacheKey, message, CURRENT_QUEUE_URL, nextQueueUrl, parsedBody) {
	const nextDepthLinks = await redis.smembers(`${cacheKey}:links`);

	await reportToServer(
		parsedBody.clientGuid,
		parsedBody.targetUrl,
		parsedBody.currentDepth,
		parsedBody.pagesCrawled,
		nextDepthLinks);
	await deleteMessage(message, CURRENT_QUEUE_URL);
	await ensureQueueExists(nextQueueUrl);
	await sendMessages(
		nextDepthLinks,
		parsedBody,
		nextQueueUrl
	);
}

async function reportToServer(clientGuid, targetUrl, currentDepth, pagesCrawled, nextDepthLinks) {
	await fetch(`${EC2_REPORT_URL}/newCrawl`, {
		method: "POST",
		headers: {
			"Content-Type": "application/json",
		},
		body: JSON.stringify({ clientGuid, targetUrl, currentDepth, pagesCrawled, nextDepthLinks }),
	});
}

async function fetchHTML(url) {
	try {
		const response = await fetch(url);
		const contentType = response.headers.get("content-type");

		if (contentType && contentType.includes("text/html")) {
			return await response.text();
		}
	} catch (error) {
		console.error(`Failed to fetch ${url}:`, error.message);
	}
	return null;
}

function extractLinks($, baseUrl, currentDepth, maxDepth) {
	if (currentDepth + 1 > maxDepth) return [];

	const nextDepthLinks = new Set();
	$("a[href]").each((_, el) => {
		try {
			const href = $(el).attr("href");
			const absoluteUrl = new URL(href, baseUrl).toString();
			if (absoluteUrl.startsWith("http")) {
				nextDepthLinks.add(absoluteUrl);
			}
		} catch (error) {
			console.warn(`Invalid link skipped: ${error.message}`);
		}
	});
	return [...nextDepthLinks];
}

async function cache(cacheKey, nextDepthLinks, isMatch) {
	await redis.set(cacheKey, isMatch ? "match" : "no-match", "EX", 86400);

	if(nextDepthLinks.length > 0) {
		const linksKey = `${cacheKey}:links`
		await redis.sadd(linksKey, ...nextDepthLinks);
		await redis.expire(linksKey, 86400);
	}
}

function isTextFound($, searchText) {
	const title = $("title").text().toLowerCase().trim();
	const textContent = $("body").text().toLowerCase().trim();
	const lowerSearchText = searchText?.toLowerCase().trim();
	return (lowerSearchText && textContent.includes(lowerSearchText)) || title.includes(lowerSearchText);
}

async function sendMessages(nextDepthLinks, parsedBody, nextQueueUrl) {
	const { currentDepth, pagesCrawled, clientGuid, maxDepth, maxPages, searchText } = parsedBody;
	if(currentDepth < maxDepth) {
		const baseBody = {
			currentDepth: currentDepth + 1,
			pagesCrawled: pagesCrawled + 1,
			clientGuid,
			maxDepth,
			maxPages,
			searchText,
		}
		const entries = nextDepthLinks.map((targetUrl, index) => ({
			Id: `${index}`,
			MessageBody: JSON.stringify({ targetUrl, ...baseBody })
		}));
		for (let i = 0; i < entries.length; i += 10) {
			const batch = entries.slice(i, i + 10);
			await sqsClient.send(
			new SendMessageBatchCommand({
				QueueUrl: nextQueueUrl,
				Entries: batch,
			}));
		}
	}
}

async function deleteIfQueueEmpty(currentQueueUrl) {
	const { Attributes } = await sqsClient.send(
		new GetQueueAttributesCommand({
			QueueUrl: currentQueueUrl,
			AttributeNames: ["ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"],
		})
	);

	const visible = parseInt(Attributes.ApproximateNumberOfMessages || "0");
	const inflight = parseInt(Attributes.ApproximateNumberOfMessagesNotVisible || "0");

	console.log(`Queue check — visible: ${visible}, inflight: ${inflight}`);

	if (visible === 0 && inflight === 0) {
		console.log("Deleting empty queue:", currentQueueUrl);
		try {
			await sqsClient.send(new DeleteQueueCommand({ QueueUrl: currentQueueUrl }));
		} catch (err) {
			console.warn(`Queue ${currentQueueUrl} might already be deleted:`, err.message);
		}
	}
}
