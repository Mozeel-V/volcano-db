# VolcanoDB Native Protocol

This document defines the initial wire protocol for VolcanoDB server mode.

## Scope

- Transport: TCP stream socket
- Encoding: UTF-8 text
- Framing: newline-delimited commands and status lines
- Session identity: derived by server from client endpoint `IP:port`
- Protocol version: v1 (text protocol)

## Connection Handshake

After TCP connect, server sends:

```text
HELLO VDB
SESSION <ip>:<port>
```

Example:

```text
HELLO VDB
SESSION 127.0.0.1:60344
```

## Commands

Each command is one line terminated by `\n`.

### Health

- Request: `PING`
- Response: `PONG`

### Disconnect

- Request: `QUIT` (also accepts `.quit` and `.exit`)
- Response: `BYE`
- Connection closes immediately after response.

### SQL execution

- Client sends SQL text lines.
- Server concatenates lines until statement terminator `;` is seen.
- If statement is incomplete, server returns:

```text
CONTINUE
```

- Once complete, server executes and returns:

Success:

```text
OK
<captured engine output lines...>
END
```

Failure:

```text
ERROR
<captured engine output lines and/or error text...>
END
```

Notes:

1. `END` marks end of one execution response.
2. Output body may be empty (for some commands).
3. Statements are currently executed serially under a global engine lock.

## Error handling

- Unknown/invalid commands are treated as SQL input and may fail at parser stage.
- Transport errors (disconnects, short writes) terminate the session.

## Protocol examples

### Example 1: ping

Client:

```text
PING
```

Server:

```text
PONG
```

### Example 2: single-line SQL

Client:

```text
SELECT 1;
```

Server:

```text
OK
... query output ...
END
```

### Example 3: multi-line SQL

Client:

```text
SELECT
```

Server:

```text
CONTINUE
```

Client:

```text
1;
```

Server:

```text
OK
... query output ...
END
```
