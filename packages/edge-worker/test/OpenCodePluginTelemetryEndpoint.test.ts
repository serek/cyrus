import Fastify from "fastify";
import { describe, expect, it, vi } from "vitest";
import {
	PATH,
	registerOpenCodePluginTelemetryEndpoint,
} from "../src/OpenCodePluginTelemetryEndpoint.js";

const token = "test-telemetry-token";
function setup() {
	const app = Fastify();
	const manager = {
		getSession: vi.fn().mockReturnValue({
			issueContext: { issueId: "issue-1" },
			agentRunner: { constructor: { name: "OpenCodeRunner" } },
		}),
		reportOptionalPluginTelemetry: vi.fn().mockResolvedValue(undefined),
	};
	expect(registerOpenCodePluginTelemetryEndpoint(app, manager, token)).toBe(
		true,
	);
	return { app, manager };
}
describe("OpenCodePluginTelemetryEndpoint", () => {
	it("forwards only an authenticated, session-affine metadata signal", async () => {
		const { app, manager } = setup();
		const response = await app.inject({
			method: "POST",
			url: PATH,
			headers: { authorization: `Bearer ${token}` },
			payload: {
				version: 1,
				agentSessionId: "agent-1",
				issueId: "issue-1",
				eventType: "tool.execute.before",
				metadata: { toolName: "bash" },
			},
		});
		expect(response.statusCode).toBe(204);
		expect(manager.reportOptionalPluginTelemetry).toHaveBeenCalledWith(
			"agent-1",
			{
				pluginId: "opencode-local-telemetry",
				kind: "tool",
				label: "Tool started · bash",
			},
		);
		await app.close();
	});
	it("rejects unauthorized, malformed, or wrong-issue signals without forwarding", async () => {
		const { app, manager } = setup();
		for (const request of [
			{ headers: {}, payload: {} },
			{ headers: { authorization: "Bearer wrong" }, payload: {} },
			{
				headers: { authorization: `Bearer ${token}` },
				payload: {
					version: 1,
					agentSessionId: "agent-1",
					issueId: "wrong",
					eventType: "session.idle",
				},
			},
			{
				headers: { authorization: `Bearer ${token}` },
				payload: {
					version: 1,
					agentSessionId: "agent-1",
					issueId: "issue-1",
					eventType: "unexpected",
				},
			},
		])
			await app.inject({ method: "POST", url: PATH, ...request });
		expect(manager.reportOptionalPluginTelemetry).not.toHaveBeenCalled();
		await app.close();
	});
	it("does not register without an operator token", () =>
		expect(
			registerOpenCodePluginTelemetryEndpoint(Fastify(), {} as any, undefined),
		).toBe(false));
});
