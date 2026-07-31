# DisPipes

A lightweight TypeScript library for creating Discord Rich Presence activities through Discord IPC.

## Installation

```bash
npm install @discord.dispipes/dispipes
```

## Basic Usage

```ts
import { RichPresence } from "@discord.dispipes/dispipes";

const rpc = new RichPresence({
    clientId: "YOUR_CLIENT_ID"
});

await rpc.connect();

rpc.setPresence({
    details: "DisPipes",
    type: 0,
    state: "Running",
    startTimestamp: new Date(),
    largeImageKey: "large_image",
    largeImageText: "DisPipes"
});
```

## Buttons

You can add up to two buttons:
> Buttons are only visible to other users — you cannot see buttons on your own Rich Presence. To test that your buttons are working, use a second account or ask a friend to view your profile.


```ts
rpc.setPresence({
    details: "DisPipes",
    state: "Running",
    buttons: [
        {
            label: "GitHub",
            url: "https://github.com/"
        },
        {
            label: "Discord",
            url: "https://discord.com/"
        }
    ]
});
```

## Clear Presence

Remove the current Rich Presence with:

```ts
rpc.clearPresence();
```

## Disconnect
When you're finished:

```ts
await rpc.disconnect();
```

## Requirements

- Node.js 18+
- Discord Desktop must be running
- A Discord Application / Client ID

> DisPipes communicates with the Discord desktop client through IPC. Discord must be running locally for Rich Presence to work.
