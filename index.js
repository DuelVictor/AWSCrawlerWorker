import * as cheerio from "cheerio";
import {
	SQSClient,
	ReceiveMessageCommand,
	DeleteMessageCommand,
	SendMessageCommand,
} from "@aws-sdk/client-sqs";
import Redis from "ioredis";

const sqsClient = new SQSClient({ region: "eu-west-1" });
const redis = new Redis({
	host: process.env.REDIS_HOST,
	port: Number(process.env.REDIS_PORT),
});
const EC2_REPORT_URL = process.env.EC2_REPORT_URL;

export async function handler() {
	const CURRENT_QUEUE_URL = process.env.CURRENT_QUEUE_URL;

	const { message, parsedBody } = await receiveMessagePayload(
		CURRENT_QUEUE_URL
	);
	if (!message) return;

	const {
		targetUrl,
		currentDepth,
		pagesCrawled,
		client,
		maxDepth,
		maxPages,
		searchText,
	} = parsedBody;

	if (currentDepth > maxDepth || pagesCrawled >= maxPages) {
		await deleteMessage(message, CURRENT_QUEUE_URL);
		return;
	}

	const cacheKey = `url:${targetUrl}`;

	const cached = await isCached(
		message,
		CURRENT_QUEUE_URL,
		client,
		targetUrl,
		currentDepth,
		pagesCrawled,
		cacheKey
	);
	if (cached) return;

	const html = await fetchHTML(targetUrl);
	if (!html) {
		await deleteMessage(message, CURRENT_QUEUE_URL);
		return;
	}

	const $ = cheerio.load(html);

	await cache(
		$,
		targetUrl,
		client,
		currentDepth,
		pagesCrawled,
		searchText,
		cacheKey
	);

	const { nextDepthLinks, nextQueueUrl } = crawl(
		$,
		targetUrl,
		currentDepth,
		maxDepth,
		client
	);
	await sendMessages(
		nextDepthLinks,
		currentDepth,
		pagesCrawled,
		client,
		maxDepth,
		maxPages,
		searchText,
		nextQueueUrl
	);

	await deleteMessage(message, CURRENT_QUEUE_URL);
}

async function receiveMessagePayload(CURRENT_QUEUE_URL) {
	const receiveMessagePayload = {
		QueueUrl: CURRENT_QUEUE_URL,
		MaxNumberOfMessages: 1,
		VisibilityTimeout: 30,
		WaitTimeSeconds: 10,
	};

	const data = await sqsClient.send(
		new ReceiveMessageCommand(receiveMessagePayload)
	);
	if (!data.Messages || data.Messages.length === 0) return;

	const message = data.Messages[0];
	const parsedBody = JSON.parse(message.Body);
	return { message, parsedBody };
}

async function isCached(
	message,
	CURRENT_QUEUE_URL,
	client,
	targetUrl,
	currentDepth,
	pagesCrawled,
	cacheKey
) {
	const alreadyCrawled = await redis.get(cacheKey);
	if (alreadyCrawled) {
		if (alreadyCrawled === "match") {
			await fetch(`${EC2_REPORT_URL}/crawler/update`, {
				method: "POST",
				headers: {
					"Content-Type": "application/json",
				},
				body: JSON.stringify({ client, targetUrl, currentDepth, pagesCrawled }),
			});
		}
		await deleteMessage(message, CURRENT_QUEUE_URL);
		return true;
	}
	return false;
}

async function cache(
	$,
	targetUrl,
	client,
	currentDepth,
	pagesCrawled,
	searchText,
	cacheKey
) {
	const title = $("title").text().trim();
	const textContent = $("body").text().toLowerCase();

	const lowerSearchText = searchText?.toLowerCase();
	const isMatch = lowerSearchText && textContent.includes(lowerSearchText);
	await redis.set(cacheKey, isMatch ? "match" : "no-match", "EX", 86400);

	if (isMatch) {
		await fetch(`${EC2_REPORT_URL}/crawler/update`, {
			method: "POST",
			headers: {
				"Content-Type": "application/json",
			},
			body: JSON.stringify({ client, targetUrl, currentDepth, pagesCrawled }),
		});
	}
}

function crawl($, targetUrl, client, currentDepth, maxDepth) {
	if (currentDepth + 1 <= maxDepth) {
		const nextQueueUrl = `https://sqs.eu-west-1.amazonaws.com/your-account-id/${client}-depth-${
			currentDepth + 1
		}-queue`;
		const nextDepthLinks = [];

		$("a[href]").each((_, el) => {
			const href = $(el).attr("href");
			try {
				const absoluteUrl = new URL(href, targetUrl).toString();
				if (
					absoluteUrl.startsWith("http://") ||
					absoluteUrl.startsWith("https://")
				) {
					nextDepthLinks.push(absoluteUrl);
				}
			} catch {}
		});
		return { nextDepthLinks, nextQueueUrl };
	}
	return [];
}

async function sendMessages(
	nextDepthLinks,
	currentDepth,
	pagesCrawled,
	client,
	maxDepth,
	maxPages,
	searchText,
	nextQueueUrl
) {
	for (const nextUrl of nextDepthLinks) {
		await sqsClient.send(
			new SendMessageCommand({
				QueueUrl: nextQueueUrl,
				MessageBody: JSON.stringify({
					targetUrl: nextUrl,
					currentDepth: currentDepth + 1,
					pagesCrawled: pagesCrawled + 1,
					client,
					maxDepth,
					maxPages,
					searchText,
				}),
			})
		);
	}
}

async function fetchHTML(url) {
	try {
		const response = await fetch(url);
		const contentType = response.headers.get("content-type");
		if (contentType && contentType.includes("text/html")) {
			return await response.text();
		}
	} catch {}
	return null;
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
