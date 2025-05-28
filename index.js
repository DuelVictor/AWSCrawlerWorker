import * as cheerio from "cheerio";
import {
	SQSClient,
	DeleteMessageCommand,
	SendMessageBatchCommand,
	CreateQueueCommand,
	GetQueueAttributesCommand,
	DeleteQueueCommand,
} from "@aws-sdk/client-sqs";
import Redis from "ioredis";

const sqsClient = new SQSClient({ region: "eu-west-1" });
const redis = new Redis({
  host: process.env.REDIS_HOST,
  port: Number(process.env.REDIS_PORT),
  connectTimeout: 1000,
  commandTimeout: 1000,
  tls: {}
});


const EC2_REPORT_URL = process.env.EC2_REPORT_URL;

export async function handler(event) {
	console.log("Lambda invoked with event:", JSON.stringify(event));

	redis.ping().then(res => console.log("Redis ping response:", res))
  		.catch(err => console.error("Redis ping error:", err));
	redis.on("error", err => console.error("Redis error:", err));

	if (!event.Records?.length) {
		console.log("No records, exiting.");
		return;
	}

	const { eventSourceARN, receiptHandle, body } = event.Records[0];
	const {CURRENT_QUEUE_URL, nextQueueUrl, queueName} = getQueueUrlFromArn(eventSourceARN);
	const parsedBody = JSON.parse(body);

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
		await deleteMessage(receiptHandle, CURRENT_QUEUE_URL);
		console.log("Deleted message, going too deep");
		return;
	}

	const cacheKey = `url:${targetUrl}`;

	if (await redis.exists(cacheKey)) {
		console.log("Started working with redis");
		handleCached(cacheKey, receiptHandle, CURRENT_QUEUE_URL, nextQueueFullUrl, parsedBody);
		console.log("Handled cached key: ", cacheKey);
		return;
	}

	const html = await fetchHTML(targetUrl);
	console.log("Fethched html");
	if (!html) {
		await deleteMessage(receiptHandle, CURRENT_QUEUE_URL);
		console.log("Not html");
		return;
	}

	const $ = cheerio.load(html);
	console.log("Loaded html");

	const nextDepthLinks = extractLinks($, targetUrl, currentDepth, maxDepth, clientGuid);
	console.log("Extracted links");

	const isMatch = isTextFound($, searchText);
	console.log("Matched test");

	await cache(cacheKey, nextDepthLinks, isMatch);
	console.log("Finished caching");

	if(isMatch) {
		await reportToServer(clientGuid, targetUrl, currentDepth, pagesCrawled, nextDepthLinks);
		console.log("Reported back to server");
	}

	await createQueue(queueName);
	console.log("Create new queue");

	await sendMessages(
		nextDepthLinks,
		parsedBody,
		nextQueueFullUrl
	);
	console.log("Sent messages");

	await deleteMessage(receiptHandle, CURRENT_QUEUE_URL);
	console.log("Deleted message");

	await deleteIfQueueEmpty(CURRENT_QUEUE_URL);
}

function getQueueUrlFromArn(arn) {
	const [, , , region, accountId, queueName] = arn.split(":");
	const CURRENT_QUEUE_URL = `https://sqs.${region}.amazonaws.com/${accountId}/${queueName}`;
	const nextQueueUrl = `https://sqs.${region}.amazonaws.com/${accountId}`;

	console.log(`Current url: ${CURRENT_QUEUE_URL} \n Next url: ${nextQueueUrl}`);

	return { CURRENT_QUEUE_URL, nextQueueUrl, queueName };
}

async function createQueue(queueName) {
	console.log(`Creating new queue: ${queueName}`);
	await sqsClient.send(new CreateQueueCommand({
		QueueName: queueName,
		Attributes: {
			VisibilityTimeout: "600",
			MessageRetentionPeriod: "1200",
		}
	}));
}

async function deleteMessage(receiptHandle, queueUrl) {
	if (!receiptHandle) return;
	await sqsClient.send(
		new DeleteMessageCommand({
			QueueUrl: queueUrl,
			ReceiptHandle: receiptHandle,
		})
	);
}

async function handleCached(cacheKey, receiptHandle, CURRENT_QUEUE_URL, nextQueueUrl, parsedBody) {
	const nextDepthLinks = await redis.smembers(`${cacheKey}:links`);

	await reportToServer(
		parsedBody.clientGuid,
		parsedBody.targetUrl,
		parsedBody.currentDepth,
		parsedBody.pagesCrawled,
		nextDepthLinks);
	await deleteMessage(receiptHandle, CURRENT_QUEUE_URL);
	await createQueue(nextQueueUrl);
	await sendMessages(nextDepthLinks, parsedBody, nextQueueUrl);
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
	console.log("Started chaching");
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
			await sqsClient.send(new SendMessageBatchCommand({
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
