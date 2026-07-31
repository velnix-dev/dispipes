import net from "node:net";
import { EventEmitter } from "node:events";
import { consola } from "consola";

export interface RichPresenceOptions {
    clientId: string;
    autoReconnect?: boolean;
    reconnectDelay?: number;
}

export interface PresenceAssets {
    largeImageKey?: string;
    largeImageText?: string;
    smallImageKey?: string;
    smallImageText?: string;
}

export interface PresenceButton {
    label: string;
    url: string;
}

export type PresenceType = 0 | 1 | 2 | 3 | 4;

export interface PresenceData {
    state?: string;
    details?: string;
    type?: PresenceType;
    startTimestamp?: number | Date;
    endTimestamp?: number | Date;
    assets?: PresenceAssets;
    buttons?: PresenceButton[];
    instance?: boolean;
    partyId?: string;
    partySize?: number;
    partyMax?: number;
    matchSecret?: string;
    joinSecret?: string;
    spectateSecret?: string;
}

interface IpcPacket {
    opcode: number;
    data: unknown;
}

interface IpcResponse {
    cmd?: string;
    evt?: string | null;
    nonce?: string;
    data?: unknown;
}

interface DiscordErrorData {
    code?: number;
    message?: string;
}

interface PendingRequest {
    resolve: (value: unknown) => void;
    reject: (reason?: unknown) => void;
    timer: NodeJS.Timeout;
}

export class DisPipesError extends Error {
    public readonly fatal: boolean;
    public readonly code: number | undefined;

    constructor(message: string, options?: { fatal?: boolean; code?: number }) {
        super(message);
        this.name = "DisPipesError";
        this.fatal = options?.fatal ?? false;
        this.code = options?.code;
        Object.setPrototypeOf(this, new.target.prototype);
    }
}

export class RichPresence extends EventEmitter {
    private static readonly IPC_OPCODE_HANDSHAKE = 0;
    private static readonly IPC_OPCODE_FRAME = 1;
    private static readonly IPC_OPCODE_CLOSE = 2;
    private static readonly IPC_OPCODE_PING = 3;
    private static readonly IPC_OPCODE_PONG = 4;
    private static readonly MAX_PACKET_SIZE = 10 * 1024 * 1024;
    private static readonly REQUEST_TIMEOUT = 10_000;

    private readonly clientId: string;
    private readonly autoReconnect: boolean;
    private readonly reconnectDelay: number;

    private socket: net.Socket | undefined;
    private receiveBuffer = Buffer.alloc(0);
    private connected = false;
    private connecting = false;
    private disconnecting = false;
    private reconnectTimer: NodeJS.Timeout | undefined;
    private reconnectAttempt = 0;
    private readonly pendingRequests = new Map<string, PendingRequest>();

    constructor(options: RichPresenceOptions) {
        super();

        if (!options || !/^\d+$/.test(options.clientId)) {
            throw new DisPipesError("clientId must be a valid Discord application ID.", {fatal: true});
        }

        this.clientId = options.clientId;
        this.autoReconnect = options.autoReconnect ?? true;
        this.reconnectDelay = Math.max(250, options.reconnectDelay ?? 3_000);

        // EventEmitter throws if an `error` event has no listener.
        // Keep library consumers safe by providing a default error listener.
        this.on("error", () => undefined);
    }

    public get isConnected(): boolean {
        return this.connected;
    }

    public async connect(): Promise<void> {
        if (this.connected) return;
        if (this.connecting) {
            throw new DisPipesError("A Discord IPC connection is already being established.");
        }

        this.clearReconnectTimer();
        this.disconnecting = false;
        this.connecting = true;

        try {
            const endpoint = await this.findEndpoint();
            await this.openSocket(endpoint);
            await this.handshake();

            this.connected = true;
            this.connecting = false;
            this.reconnectAttempt = 0;

            consola.success("DisPipes connected to Discord.");
            this.emit("ready");
        } catch (error: unknown) {
            this.connecting = false;
            this.connected = false;

            const normalized = this.normalizeError(error, "Failed to connect to Discord.");
            this.reportError(normalized);
            this.cleanupSocket();
            this.scheduleReconnectIfNeeded(normalized);
            throw normalized;
        }
    }

    public async setPresence(presence: PresenceData): Promise<void> {
        if (!this.connected) {
            throw new DisPipesError("Cannot set presence while DisPipes is disconnected.");
        }

        this.validatePresence(presence);

        const activity: Record<string, unknown> = {};

        if (presence.state !== undefined) activity.state = presence.state;
        if (presence.details !== undefined) activity.details = presence.details;
        if (presence.type !== undefined) activity.type = presence.type;

        if (presence.startTimestamp !== undefined || presence.endTimestamp !== undefined) {
            const timestamps: Record<string, number> = {};

            if (presence.startTimestamp !== undefined) {
                timestamps.start = this.timestamp(presence.startTimestamp);
            }
            if (presence.endTimestamp !== undefined) {
                timestamps.end = this.timestamp(presence.endTimestamp);
            }

            activity.timestamps = timestamps;
        }

        if (presence.assets !== undefined) {
            const assets: Record<string, string> = {};

            if (presence.assets.largeImageKey !== undefined) assets.large_image = presence.assets.largeImageKey;
            if (presence.assets.largeImageText !== undefined) assets.large_text = presence.assets.largeImageText;
            if (presence.assets.smallImageKey !== undefined) assets.small_image = presence.assets.smallImageKey;
            if (presence.assets.smallImageText !== undefined) assets.small_text = presence.assets.smallImageText;

            if (Object.keys(assets).length > 0) activity.assets = assets;
        }

        if (presence.buttons !== undefined && presence.buttons.length > 0) {
            activity.buttons = presence.buttons.map((button) => ({
                label: button.label,
                url: button.url
            }));
        }

        if (
            presence.partyId !== undefined ||
            presence.partySize !== undefined ||
            presence.partyMax !== undefined
        ) {
            const party: Record<string, unknown> = {};

            if (presence.partyId !== undefined) party.id = presence.partyId;
            if (presence.partySize !== undefined || presence.partyMax !== undefined) {
                party.size = [presence.partySize ?? 0, presence.partyMax ?? 0];
            }

            activity.party = party;
        }

        if (
            presence.matchSecret !== undefined ||
            presence.joinSecret !== undefined ||
            presence.spectateSecret !== undefined
        ) {
            const secrets: Record<string, string> = {};

            if (presence.matchSecret !== undefined) secrets.match = presence.matchSecret;
            if (presence.joinSecret !== undefined) secrets.join = presence.joinSecret;
            if (presence.spectateSecret !== undefined) secrets.spectate = presence.spectateSecret;

            activity.secrets = secrets;
        }

        if (presence.instance !== undefined) activity.instance = presence.instance;

        await this.sendCommand("SET_ACTIVITY", {
            pid: process.pid,
            activity
        });

        this.emit("presenceUpdate", presence);
    }

    public async clearPresence(): Promise<void> {
        if (!this.connected) return;

        await this.sendCommand("SET_ACTIVITY", {
            pid: process.pid,
            activity: null
        });

        this.emit("presenceClear");
    }

    public async disconnect(): Promise<void> {
        this.disconnecting = true;
        this.clearReconnectTimer();

        try {
            if (this.connected && this.socket !== undefined && !this.socket.destroyed) {
                await this.sendCommand("SET_ACTIVITY", {
                    pid: process.pid,
                    activity: null
                }).catch(() => undefined);
            }
        } finally {
            this.rejectPendingRequests(new DisPipesError("DisPipes disconnected."));
            this.cleanupSocket();
            this.connected = false;
            this.connecting = false;
            this.reconnectAttempt = 0;
            this.disconnecting = false;
            this.emit("disconnect", undefined);
            consola.info("DisPipes disconnected from Discord.");
        }
    }

    private async findEndpoint(): Promise<string> {
        for (const endpoint of this.getEndpoints()) {
            try {
                await this.testSocket(endpoint);
                return endpoint;
            } catch {
                // Try the next Discord IPC endpoint.
            }
        }

        throw new DisPipesError(
            "Discord is not running or no Discord IPC endpoint could be opened."
        );
    }

    private getEndpoints(): string[] {
        if (process.platform === "win32") {
            return Array.from(
                {length: 10},
                (_, index) => `\\\\?\\pipe\\discord-ipc-${index}`
            );
        }

        const runtimeDirs = [
            process.env.XDG_RUNTIME_DIR,
            process.env.TMPDIR,
            "/tmp"
        ].filter((directory): directory is string => directory !== undefined && directory.length > 0);

        const uniqueDirs = [...new Set(
            runtimeDirs.map((directory) => directory.replace(/\/$/, ""))
        )];

        return uniqueDirs.flatMap((directory) =>
            Array.from(
                {length: 10},
                (_, index) => `${directory}/discord-ipc-${index}`
            )
        );
    }

    private async testSocket(endpoint: string): Promise<void> {
        await new Promise<void>((resolve, reject) => {
            const socket = net.createConnection(endpoint);
            let settled = false;

            const finish = (callback: () => void): void => {
                if (settled) return;
                settled = true;
                clearTimeout(timeout);
                socket.removeAllListeners();
                socket.destroy();
                callback();
            };

            const timeout = setTimeout(() => {
                finish(() => reject(new Error("IPC connection timed out.")));
            }, 500);

            socket.once("connect", () => {
                finish(resolve);
            });

            socket.once("error", (error: Error) => {
                finish(() => reject(error));
            });
        });
    }

    private async openSocket(endpoint: string): Promise<void> {
        await new Promise<void>((resolve, reject) => {
            const socket = net.createConnection(endpoint);
            let settled = false;

            this.socket = socket;
            this.receiveBuffer = Buffer.alloc(0);

            const cleanupConnectListeners = (): void => {
                clearTimeout(timeout);
                socket.off("error", onConnectError);
                socket.off("connect", onConnect);
            };

            const onConnect = (): void => {
                if (settled) return;
                settled = true;
                cleanupConnectListeners();
                resolve();
            };

            const onConnectError = (error: Error): void => {
                if (settled) return;
                settled = true;
                cleanupConnectListeners();
                reject(error);
            };

            const timeout = setTimeout(() => {
                if (settled) return;
                settled = true;
                cleanupConnectListeners();
                socket.destroy();
                reject(new Error("Discord IPC connection timed out."));
            }, 5_000);

            socket.once("connect", onConnect);
            socket.once("error", onConnectError);
            socket.on("data", (chunk: Buffer) => this.handleData(chunk));
            socket.on("error", (error: Error) => this.handleSocketError(error));
            socket.on("close", () => this.handleSocketClose());
        });
    }

    private async handshake(): Promise<void> {
        const socket = this.getSocket();

        await this.writePacket(
            socket,
            this.createPacket(RichPresence.IPC_OPCODE_HANDSHAKE, {
                v: 1,
                client_id: this.clientId
            })
        );

        await new Promise<void>((resolve, reject) => {
            let settled = false;

            const cleanup = (): void => {
                clearTimeout(timeout);
                this.off("READY", onReady);
                this.off("error", onError);
            };

            const finish = (callback: () => void): void => {
                if (settled) return;
                settled = true;
                cleanup();
                callback();
            };

            const onReady = (): void => {
                finish(resolve);
            };

            const onError = (error: unknown): void => {
                const normalized = this.normalizeError(error, "Discord IPC handshake failed.");
                finish(() => reject(normalized));
            };

            const timeout = setTimeout(() => {
                finish(() => reject(new DisPipesError("Discord IPC handshake timed out.")));
            }, RichPresence.REQUEST_TIMEOUT);

            this.once("READY", onReady);
            this.once("error", onError);
        });
    }

    private async sendCommand(command: string, args?: unknown): Promise<unknown> {
        return this.sendRequest(RichPresence.IPC_OPCODE_FRAME, {
            cmd: command,
            args: args ?? {},
            nonce: this.createNonce()
        });
    }

    private getSocket(): net.Socket {
        const socket = this.socket;

        if (socket === undefined || socket.destroyed) {
            throw new DisPipesError("Discord IPC socket is not available.");
        }

        return socket;
    }

    private createPacket(opcode: number, data: unknown): Buffer {
        const payload = Buffer.from(JSON.stringify(data), "utf8");

        if (payload.length > RichPresence.MAX_PACKET_SIZE) {
            throw new DisPipesError("IPC payload exceeds the maximum allowed packet size.");
        }

        const header = Buffer.allocUnsafe(8);
        header.writeUInt32LE(opcode, 0);
        header.writeUInt32LE(payload.length, 4);

        return Buffer.concat([header, payload]);
    }

    private async sendRequest(
        opcode: number,
        data: Record<string, unknown>,
        waitForResponse = false
    ): Promise<unknown> {
        const socket = this.getSocket();
        const packet = this.createPacket(opcode, data);

        const nonce = typeof data.nonce === "string" ? data.nonce : undefined;

        if (!waitForResponse && nonce === undefined) {
            await this.writePacket(socket, packet);
            return undefined;
        }

        if (nonce === undefined) {
            throw new DisPipesError("Internal IPC request is missing a nonce.", {fatal: true});
        }

        return new Promise<unknown>((resolve, reject) => {
            const timer = setTimeout(() => {
                this.pendingRequests.delete(nonce);
                reject(new DisPipesError(`Discord IPC request timed out: ${String(data.cmd ?? "HANDSHAKE")}`));
            }, RichPresence.REQUEST_TIMEOUT);

            this.pendingRequests.set(nonce, {resolve, reject, timer});

            void this.writePacket(socket, packet).catch((error: unknown) => {
                const pending = this.pendingRequests.get(nonce);
                if (pending === undefined) return;
                clearTimeout(pending.timer);
                this.pendingRequests.delete(nonce);
                pending.reject(error);
            });
        });
    }

    private async writePacket(socket: net.Socket, packet: Buffer): Promise<void> {
        await new Promise<void>((resolve, reject) => {
            socket.write(packet, (error?: Error | null) => {
                if (error !== undefined && error !== null) reject(error);
                else resolve();
            });
        });
    }

    private handleData(chunk: Buffer): void {
        const buffer = Buffer.from(chunk);
        this.receiveBuffer = Buffer.concat([this.receiveBuffer, buffer]);

        while (this.receiveBuffer.length >= 8) {
            const opcode = this.receiveBuffer.readUInt32LE(0);
            const length = this.receiveBuffer.readUInt32LE(4);

            if (length > RichPresence.MAX_PACKET_SIZE) {
                this.handleFatalError(
                    new DisPipesError("Discord sent an IPC packet exceeding the maximum size.", {fatal: true})
                );
                return;
            }

            const packetSize = 8 + length;
            if (this.receiveBuffer.length < packetSize) return;

            const payload = this.receiveBuffer.subarray(8, packetSize).toString("utf8");
            this.receiveBuffer = this.receiveBuffer.subarray(packetSize);

            let data: unknown;
            try {
                data = JSON.parse(payload) as unknown;
            } catch {
                this.handleFatalError(
                    new DisPipesError("Discord sent invalid JSON over IPC.", {fatal: true})
                );
                return;
            }

            this.handlePacket({opcode, data});
        }
    }

    private handlePacket(packet: IpcPacket): void {
        if (packet.opcode === RichPresence.IPC_OPCODE_CLOSE) {
            this.handleFatalError(
                new DisPipesError("Discord closed the IPC connection.", {fatal: false})
            );
            return;
        }

        if (packet.opcode === RichPresence.IPC_OPCODE_PING) {
            void this.sendPacket(RichPresence.IPC_OPCODE_PONG, packet.data).catch((error: unknown) => {
                this.handleError(error);
            });
            return;
        }

        if (packet.opcode !== RichPresence.IPC_OPCODE_FRAME) {
            this.emit("packet", packet);
            return;
        }

        if (!this.isRecord(packet.data)) {
            this.handleFatalError(
                new DisPipesError("Discord sent a malformed IPC frame.", {fatal: true})
            );
            return;
        }

        const response = packet.data as IpcResponse;

        if (response.evt === "ERROR") {
            const errorData = this.isRecord(response.data)
                ? (response.data as DiscordErrorData)
                : {};
            this.handleError(this.classifyDiscordError(errorData));
            return;
        }

        if (response.nonce !== undefined) {
            const pending = this.pendingRequests.get(response.nonce);
            if (pending !== undefined) {
                clearTimeout(pending.timer);
                this.pendingRequests.delete(response.nonce);

                if (response.evt === "ERROR") {
                    pending.reject(this.classifyDiscordError(
                        this.isRecord(response.data) ? (response.data as DiscordErrorData) : {}
                    ));
                } else {
                    pending.resolve(response.data);
                }
            }
        }

        if (response.evt !== undefined && response.evt !== null) {
            this.emit(response.evt, response.data);
            this.emit("event", response.evt, response.data);
        }

        this.emit("packet", packet);
    }

    private async sendPacket(opcode: number, data: unknown): Promise<void> {
        const socket = this.getSocket();
        await this.writePacket(socket, this.createPacket(opcode, data));
    }

    private classifyDiscordError(data: DiscordErrorData): DisPipesError {
        const message = data.message ?? "Discord returned an IPC error.";
        const options: { fatal?: boolean; code?: number } = {fatal: false};

        if (data.code !== undefined) options.code = data.code;

        return new DisPipesError(message, options);
    }

    private handleSocketError(error: Error): void {
        const normalized = this.normalizeError(error, "Discord IPC socket error.");
        this.reportError(normalized);
    }

    private handleSocketClose(): void {
        const wasConnected = this.connected;
        const intentional = this.disconnecting;

        this.cleanupSocket();
        this.connected = false;
        this.connecting = false;

        if (wasConnected || !intentional) {
            const error = new DisPipesError(
                intentional
                    ? "Discord IPC connection closed."
                    : "Discord IPC connection closed unexpectedly."
            );

            this.emit("disconnect", error);

            if (!intentional) {
                this.scheduleReconnectIfNeeded(error);
            }
        }
    }

    private handleError(error: unknown): void {
        const normalized = this.normalizeError(error, "An unknown DisPipes error occurred.");
        this.reportError(normalized);

        if (normalized.fatal) {
            this.handleFatalError(normalized);
        }
    }

    private handleFatalError(error: DisPipesError): void {
        this.reportError(error);
        this.rejectPendingRequests(error);
        this.cleanupSocket();
        this.connected = false;
        this.connecting = false;
        this.emit("fatalError", error);
    }

    private reportError(error: DisPipesError): void {
        if (error.fatal) {
            consola.error(`[FATAL] ${error.message}`, error.code !== undefined ? `(code ${error.code})` : "");
        } else {
            consola.warn(`[RECOVERABLE] ${error.message}`, error.code !== undefined ? `(code ${error.code})` : "");
        }

        this.emit("error", error);
    }

    private scheduleReconnectIfNeeded(error: DisPipesError): void {
        if (
            !this.autoReconnect ||
            error.fatal ||
            this.disconnecting ||
            this.reconnectTimer !== undefined
        ) {
            return;
        }

        this.reconnectAttempt += 1;
        const delay = Math.min(
            this.reconnectDelay * Math.pow(2, this.reconnectAttempt - 1),
            30_000
        );

        consola.info(
            `Discord IPC reconnect scheduled in ${delay}ms (attempt ${this.reconnectAttempt}).`
        );
        this.emit("reconnecting", {attempt: this.reconnectAttempt, delay});

        this.reconnectTimer = setTimeout(() => {
            this.reconnectTimer = undefined;
            void this.connect().catch(() => undefined);
        }, delay);
    }

    private rejectPendingRequests(error: DisPipesError): void {
        for (const [nonce, pending] of this.pendingRequests) {
            clearTimeout(pending.timer);
            pending.reject(error);
            this.pendingRequests.delete(nonce);
        }
    }

    private normalizeError(error: unknown, fallback: string): DisPipesError {
        if (error instanceof DisPipesError) return error;
        if (error instanceof Error) return new DisPipesError(error.message || fallback);
        return new DisPipesError(fallback);
    }

    private validatePresence(presence: PresenceData): void {
        if (presence.buttons !== undefined && presence.buttons.length > 2) {
            throw new DisPipesError("Discord allows a maximum of 2 presence buttons.");
        }

        for (const button of presence.buttons ?? []) {
            if (button.label.trim().length === 0) {
                throw new DisPipesError("Presence button labels cannot be empty.");
            }

            let url: URL;

            try {
                url = new URL(button.url);
            } catch {
                throw new DisPipesError(`Invalid presence button URL: ${button.url}`);
            }

            if (url.protocol !== "http:" && url.protocol !== "https:") {
                throw new DisPipesError(`Invalid presence button URL: ${button.url}`);
            }
        }

        if (presence.type !== undefined && ![0, 1, 2, 3, 4].includes(presence.type)) {
            throw new DisPipesError("Presence type must be one of Discord's supported activity types: 0, 1, 2, 3, or 4.");
        }

        for (const value of [presence.details, presence.state]) {
            if (value !== undefined && value.length > 128) {
                throw new DisPipesError("Presence details and state must be 128 characters or fewer.");
            }
        }

        if (
            presence.partySize !== undefined &&
            (!Number.isInteger(presence.partySize) || presence.partySize < 0)
        ) {
            throw new DisPipesError("partySize must be a non-negative integer.");
        }

        if (
            presence.partyMax !== undefined &&
            (!Number.isInteger(presence.partyMax) || presence.partyMax < 0)
        ) {
            throw new DisPipesError("partyMax must be a non-negative integer.");
        }

        if (
            presence.partySize !== undefined &&
            presence.partyMax !== undefined &&
            presence.partySize > presence.partyMax
        ) {
            throw new DisPipesError("partySize cannot be greater than partyMax.");
        }
    }

    private timestamp(value: number | Date): number {
        const timestamp = value instanceof Date ? value.getTime() : value;

        if (!Number.isFinite(timestamp)) {
            throw new DisPipesError("Invalid presence timestamp.");
        }

        return Math.floor(timestamp);
    }

    private createNonce(): string {
        return `${Date.now()}-${process.pid}-${Math.random().toString(36).slice(2)}`;
    }

    private isRecord(value: unknown): value is Record<string, unknown> {
        return typeof value === "object" && value !== null && !Array.isArray(value);
    }

    private clearReconnectTimer(): void {
        if (this.reconnectTimer !== undefined) {
            clearTimeout(this.reconnectTimer);
            this.reconnectTimer = undefined;
        }
    }

    private cleanupSocket(): void {
        this.rejectPendingRequests(new DisPipesError("Discord IPC socket closed."));

        if (this.socket !== undefined) {
            this.socket.removeAllListeners();
            this.socket.destroy();
        }

        this.socket = undefined;
        this.receiveBuffer = Buffer.alloc(0);
    }
}
