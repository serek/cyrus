import { timingSafeEqual } from "node:crypto";
import type { CyrusAgentSession } from "cyrus-core";
import type { FastifyInstance } from "fastify";
import type {
	OptionalPluginTelemetryKind,
	OptionalPluginTelemetrySignal,
} from "./SessionObservabilityProjection.js";

const MAX_BODY_BYTES = 4_096;
const PATH = "/internal/opencode-plugin-telemetry";
const IDENTIFIER = /^[A-Za-z0-9._:-]{1,200}$/;
const EVENT_MAP: Record<
	string,
	{ kind: OptionalPluginTelemetryKind; label: string }
> = {
	"session.created": { kind: "phase", label: "OpenCode started" },
	"session.status": {
		kind: "phase",
		label: "OpenCode status changed",
	},
	"session.idle": { kind: "final", label: "OpenCode idle" },
	"session.error": {
		kind: "final",
		label: "OpenCode error",
	},
	"todo.updated": { kind: "gate", label: "Todo list updated" },
	"tool.execute.before": { kind: "tool", label: "Tool started" },
	"tool.execute.after": { kind: "tool", label: "Tool finished" },
	"message.updated": { kind: "phase", label: "OpenCode thinking" },
	"message.part.updated": {
		kind: "phase",
		label: "OpenCode thinking",
	},
	"message.part.removed": {
		kind: "phase",
		label: "OpenCode thinking updated",
	},
	"message.removed": { kind: "phase", label: "OpenCode thinking removed" },
};
type TelemetryManager = {
	getSession(sessionId: string): CyrusAgentSession | undefined;
	reportOptionalPluginTelemetry(
		sessionId: string,
		signal: OptionalPluginTelemetrySignal,
	): Promise<void>;
};
type Payload = {
	version: 1;
	agentSessionId: string;
	issueId: string;
	eventType: keyof typeof EVENT_MAP;
	metadata?: {
		status?: "idle" | "busy" | "retry";
		todo?: {
			total: number;
			pending: number;
			inProgress: number;
			completed: number;
			cancelled: number;
		};
		toolName?: string;
	};
};
function isPayload(value: unknown): value is Payload {
	if (!value || typeof value !== "object" || Array.isArray(value)) return false;
	const record = value as Record<string, unknown>;
	const metadata = record.metadata;
	if (metadata !== undefined) {
		if (!metadata || typeof metadata !== "object" || Array.isArray(metadata))
			return false;
		const metadataRecord = metadata as Record<string, unknown>;
		if (
			Object.keys(metadataRecord).some(
				(key) => !["status", "todo", "toolName"].includes(key),
			)
		)
			return false;
		if (
			metadataRecord.status !== undefined &&
			!["idle", "busy", "retry"].includes(String(metadataRecord.status))
		)
			return false;
		if (
			metadataRecord.toolName !== undefined &&
			(typeof metadataRecord.toolName !== "string" ||
				metadataRecord.toolName.length > 128)
		)
			return false;
		if (metadataRecord.todo !== undefined) {
			const todo = metadataRecord.todo;
			if (!todo || typeof todo !== "object" || Array.isArray(todo))
				return false;
			const todoRecord = todo as Record<string, unknown>;
			if (
				Object.keys(todoRecord).some(
					(key) =>
						![
							"total",
							"pending",
							"inProgress",
							"completed",
							"cancelled",
						].includes(key),
				) ||
				["total", "pending", "inProgress", "completed", "cancelled"].some(
					(key) =>
						typeof todoRecord[key] !== "number" ||
						!Number.isInteger(todoRecord[key]) ||
						(todoRecord[key] as number) < 0 ||
						(todoRecord[key] as number) > 999,
				)
			)
				return false;
		}
	}
	return (
		Object.keys(record).every((key) =>
			[
				"version",
				"agentSessionId",
				"issueId",
				"eventType",
				"metadata",
			].includes(key),
		) &&
		Object.keys(record).length >= 4 &&
		record.version === 1 &&
		typeof record.agentSessionId === "string" &&
		IDENTIFIER.test(record.agentSessionId) &&
		typeof record.issueId === "string" &&
		IDENTIFIER.test(record.issueId) &&
		typeof record.eventType === "string" &&
		record.eventType in EVENT_MAP
	);
}
function isAuthorized(header: unknown, token: string): boolean {
	if (typeof header !== "string" || !header.startsWith("Bearer ")) return false;
	const supplied = Buffer.from(header.slice("Bearer ".length));
	const expected = Buffer.from(token);
	return (
		supplied.length === expected.length && timingSafeEqual(supplied, expected)
	);
}
function isMatchingOpenCodeSession(
	session: CyrusAgentSession,
	issueId: string,
): boolean {
	return (
		(session.issueContext?.issueId ?? session.issueId) === issueId &&
		Boolean(
			session.opencodeSessionId ||
				session.agentRunner?.constructor.name === "OpenCodeRunner",
		)
	);
}
/** Registers a private, metadata-only ingress for the optional OpenCode plugin. */
export function registerOpenCodePluginTelemetryEndpoint(
	fastify: FastifyInstance,
	manager: TelemetryManager,
	token: string | undefined,
): boolean {
	if (!token?.trim()) return false;
	fastify.post(PATH, { bodyLimit: MAX_BODY_BYTES }, async (request, reply) => {
		if (!isAuthorized(request.headers.authorization, token))
			return reply.status(401).send();
		if (!isPayload(request.body)) return reply.status(400).send();
		const session = manager.getSession(request.body.agentSessionId);
		if (!session || !isMatchingOpenCodeSession(session, request.body.issueId))
			return reply.status(404).send();
		const event = EVENT_MAP[request.body.eventType];
		if (!event) return reply.status(400).send();
		const metadata = request.body.metadata;
		const label =
			metadata?.toolName && event.kind === "tool"
				? `${event.label} · ${metadata.toolName}`
				: metadata?.todo && event.kind === "gate"
					? `Todo ${metadata.todo.completed}/${metadata.todo.total} complete`
					: metadata?.status && event.kind === "phase"
						? `OpenCode ${metadata.status}`
						: event.label;
		await manager.reportOptionalPluginTelemetry(request.body.agentSessionId, {
			pluginId: "opencode-local-telemetry",
			kind: event.kind,
			label,
			phase: metadata?.status,
			todo: metadata?.todo
				? {
						completed: metadata.todo.completed,
						total: metadata.todo.total,
					}
				: undefined,
		});
		return reply.status(204).send();
	});
	return true;
}
export { MAX_BODY_BYTES, PATH };
