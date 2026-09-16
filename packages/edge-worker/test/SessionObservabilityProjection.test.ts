import { describe, expect, it } from "vitest";
import { SessionObservabilityProjection } from "../src/SessionObservabilityProjection";

describe("SessionObservabilityProjection", () => {
	it("distinguishes an alive runner with stale output from an unexpected exit at the threshold", () => {
		const projection = new SessionObservabilityProjection({
			staleOutputAfterMs: 1_000,
		});

		projection.recordRunner({ type: "output", at: 100 });
		expect(projection.takeHealthActivity(100)).toEqual({
			type: "thought",
			body: "Runner alive.",
		});
		expect(projection.takeHealthActivity(1_099)).toBeNull();
		expect(projection.takeHealthActivity(1_100)).toEqual({
			type: "thought",
			body: "Runner alive; output stale.",
		});

		projection.recordRunner({ type: "unexpected_exit", at: 1_101 });
		expect(projection.takeHealthActivity(1_101)).toEqual({
			type: "error",
			body: "Runner exited unexpectedly.",
		});
		expect(projection.takeHealthActivity(1_102)).toBeNull();
	});

	it("projects phase, elapsed time, activity, todos, attention, and recovery", () => {
		const projection = new SessionObservabilityProjection({ startedAt: 100 });
		projection.recordPlugin({
			pluginId: "planner",
			kind: "phase",
			label: "Plan ready",
			at: 125,
			phase: "planning",
			todo: { completed: 3, total: 2 },
			attention: "needs_input",
			recovery: { state: "retrying", attempt: 2 },
		});
		expect(projection.status(200)).toEqual({
			phase: "planning",
			elapsedMs: 100,
			lastMeaningfulActivity: { label: "Plan ready", at: 125 },
			todo: { completed: 2, total: 2 },
			attention: ["needs_input"],
			recovery: { state: "retrying", attempt: 2 },
		});
	});

	it("keeps runner liveness independent from optional telemetry", () => {
		const projection = new SessionObservabilityProjection();
		projection.recordRunner({ type: "alive", at: 1 });
		expect(projection.takeHealthActivity(1)?.body).toBe("Runner alive.");

		expect(
			projection.recordPlugin({
				pluginId: "reviewer",
				kind: "review",
				label: "Review started",
				availability: "unavailable",
			}),
		).toEqual({ type: "thought", body: "Review started" });
		expect(projection.takeHealthActivity(2)?.body).toBe(
			"Runner alive. Telemetry unavailable: reviewer.",
		);
		expect(
			projection.recordPlugin({
				pluginId: "reviewer",
				kind: "review",
				label: "Review started",
				availability: "unavailable",
			}),
		).toBeNull();
		expect(projection.takeHealthActivity(3)).toBeNull();

		projection.recordPlugin({
			pluginId: "reviewer",
			kind: "final",
			label: "Review telemetry restored",
			availability: "available",
		});
		expect(projection.takeHealthActivity(4)?.body).toBe(
			"Runner alive. Telemetry restored: reviewer.",
		);
	});

	it("isolates concurrent session projections", () => {
		const alpha = new SessionObservabilityProjection({ startedAt: 0 });
		const beta = new SessionObservabilityProjection({ startedAt: 0 });
		alpha.recordPlugin({
			pluginId: "alpha",
			kind: "phase",
			label: "Alpha",
			at: 1,
			phase: "build",
			attention: "permission_blocked",
		});
		beta.recordPlugin({
			pluginId: "beta",
			kind: "phase",
			label: "Beta",
			at: 1,
			phase: "review",
		});
		expect(alpha.status(2)).toMatchObject({
			phase: "build",
			attention: ["permission_blocked"],
		});
		expect(beta.status(2)).toMatchObject({ phase: "review", attention: [] });
	});
});
