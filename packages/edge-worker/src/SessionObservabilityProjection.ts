/**
 * A runner's liveness is independent from optional integrations. In particular,
 * an unavailable plugin is degraded telemetry, never evidence that the runner
 * process has stopped.
 */
export type RunnerHealthSignal =
	| { type: "alive"; at?: number }
	| { type: "output"; at?: number }
	| { type: "unexpected_exit"; at?: number }
	| { type: "snapshot"; snapshot: RunnerHealthSnapshot };

/** Minimal runner data; no runner implementation is coupled to this projection. */
export interface RunnerHealthSnapshot {
	state: "running" | "exited" | string;
	turn: number;
	pid?: number | null;
	lastEvent?: string | null;
	lastEventAt?: number | null;
}

export type OptionalPluginTelemetryKind =
	| "phase"
	| "tool"
	| "gate"
	| "review"
	| "final";

export type ObservabilityAttention =
	| "needs_input"
	| "permission_blocked"
	| "telemetry_unavailable";
export type ObservabilityRecovery =
	| "none"
	| "retrying"
	| "recovering"
	| "failed";

/**
 * Provider-neutral contract for plugins which can enrich an agent timeline.
 * `availability` is optional because semantic events may be emitted by a
 * plugin which has no availability probe. Plugins must never use this contract
 * to request runner lifecycle actions.
 */
export interface OptionalPluginTelemetrySignal {
	pluginId: string;
	kind: OptionalPluginTelemetryKind;
	label: string;
	at?: number;
	availability?: "available" | "unavailable";
	phase?: string;
	todo?: { completed: number; total: number };
	attention?: ObservabilityAttention | null;
	recovery?: { state: ObservabilityRecovery; attempt?: number };
}

export interface ObservabilityActivity {
	type: "thought" | "error";
	body: string;
}

export interface SessionObservabilityProjectionOptions {
	staleOutputAfterMs?: number;
	startedAt?: number;
}

export interface SessionRollingStatus {
	phase: string | null;
	elapsedMs: number;
	lastMeaningfulActivity: { label: string; at: number } | null;
	todo: { completed: number; total: number } | null;
	attention: ObservabilityAttention[];
	recovery: { state: ObservabilityRecovery; attempt: number | null };
}

/**
 * Converts low-level health and optional-plugin facts into a compact,
 * Linear-facing activity stream. One instance belongs to exactly one agent
 * session; callers own the session-to-instance mapping.
 */
export class SessionObservabilityProjection {
	private readonly staleOutputAfterMs: number;
	private readonly startedAt: number;
	private runnerState: "unknown" | "alive" | "unexpected_exit" = "unknown";
	private lastOutputAt?: number;
	private unavailablePlugins = new Set<string>();
	private restoredPlugins = new Set<string>();
	private lastHealthFingerprint?: string;
	private lastSemanticFingerprint?: string;
	private phase: string | null = null;
	private lastMeaningfulActivity: { label: string; at: number } | null = null;
	private todo: { completed: number; total: number } | null = null;
	private attention = new Set<ObservabilityAttention>();
	private recovery: SessionRollingStatus["recovery"] = {
		state: "none",
		attempt: null,
	};

	constructor(options: SessionObservabilityProjectionOptions = {}) {
		this.staleOutputAfterMs = options.staleOutputAfterMs ?? 60_000;
		this.startedAt = options.startedAt ?? Date.now();
	}

	recordRunner(signal: RunnerHealthSignal): void {
		if (signal.type === "snapshot") {
			const snapshot = signal.snapshot;
			if (snapshot.state === "exited") {
				this.runnerState = "unexpected_exit";
				return;
			}
			this.runnerState = "alive";
			this.lastOutputAt = snapshot.lastEventAt ?? undefined;
			return;
		}
		const at = signal.at ?? Date.now();
		if (signal.type === "unexpected_exit") {
			this.runnerState = "unexpected_exit";
			return;
		}

		this.runnerState = "alive";
		if (signal.type === "output") this.lastOutputAt = at;
	}

	recordPlugin(
		signal: OptionalPluginTelemetrySignal,
	): ObservabilityActivity | null {
		const at = signal.at ?? Date.now();
		if (signal.availability === "unavailable") {
			this.unavailablePlugins.add(signal.pluginId);
			this.restoredPlugins.delete(signal.pluginId);
			this.attention.add("telemetry_unavailable");
		} else if (signal.availability === "available") {
			if (this.unavailablePlugins.delete(signal.pluginId)) {
				this.restoredPlugins.add(signal.pluginId);
			}
			if (this.unavailablePlugins.size === 0)
				this.attention.delete("telemetry_unavailable");
		}
		if (signal.phase !== undefined) this.phase = signal.phase;
		if (signal.todo !== undefined) {
			const total = Math.max(0, Math.floor(signal.todo.total));
			this.todo = {
				completed: Math.min(
					total,
					Math.max(0, Math.floor(signal.todo.completed)),
				),
				total,
			};
		}
		if (signal.attention === null) this.attention.clear();
		else if (signal.attention) this.attention.add(signal.attention);
		if (signal.recovery) {
			this.recovery = {
				state: signal.recovery.state,
				attempt: signal.recovery.attempt ?? null,
			};
		}
		this.lastMeaningfulActivity = { label: signal.label, at };

		const fingerprint = `${signal.pluginId}:${signal.kind}:${signal.label}`;
		if (fingerprint === this.lastSemanticFingerprint) return null;
		this.lastSemanticFingerprint = fingerprint;
		return { type: "thought", body: signal.label };
	}

	/** A compact, structured view for one session only. */
	status(now = Date.now()): SessionRollingStatus {
		return {
			phase: this.phase,
			elapsedMs: Math.max(0, now - this.startedAt),
			lastMeaningfulActivity: this.lastMeaningfulActivity && {
				...this.lastMeaningfulActivity,
			},
			todo: this.todo && { ...this.todo },
			attention: [...this.attention].sort(),
			recovery: { ...this.recovery },
		};
	}

	/** Returns a changed health summary, or null when Linear already has it. */
	takeHealthActivity(now = Date.now()): ObservabilityActivity | null {
		let body: string;
		let type: ObservabilityActivity["type"] = "thought";
		if (this.runnerState === "unexpected_exit") {
			body = "Runner exited unexpectedly.";
			type = "error";
		} else if (this.runnerState === "alive") {
			const stale =
				this.lastOutputAt !== undefined &&
				now - this.lastOutputAt >= this.staleOutputAfterMs;
			body = stale ? "Runner alive; output stale." : "Runner alive.";
		} else {
			return null;
		}

		const unavailable = [...this.unavailablePlugins].sort();
		if (unavailable.length > 0) {
			body += ` Telemetry unavailable: ${unavailable.join(", ")}.`;
		}
		const restored = [...this.restoredPlugins].sort();
		if (restored.length > 0) {
			body += ` Telemetry restored: ${restored.join(", ")}.`;
		}

		const fingerprint = `${type}:${body}`;
		if (fingerprint === this.lastHealthFingerprint) return null;
		this.lastHealthFingerprint = fingerprint;
		this.restoredPlugins.clear();
		return { type, body };
	}
}
