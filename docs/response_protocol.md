Response protocol is used to communicate between a worker and RR.
When a worker completes its job, it should send a typed response.
The response should contain:

1. `type` field with the message type. Can be treated as enums.
2. `data` field with the dynamic response related to the type.

Types are:

```
0 - NO_ERROR [DEPRECATED], replaced with ACK
1 - ERROR [DEPRECATED], replaced with NACK/REQUEUE
2 - ACK
3 - NACK
4 - REQUEUE
```

#### `NO_ERROR`: contains only `type` - 0, and empty `data`.  
example:
```json
{
  "type": 0,
  "data": {}
}
```
---

#### `ERROR` : contains `type` - 1, and `data` field with: 
- `message` describing the error (type: string).  
- `requeue` flag to requeue the job (type: boolean).  
- `delay_seconds`: to delay a queue for a provided amount of seconds (type: integer).   
- `headers` - job's headers represented as hashmap with string key and array of strings as a value. (type `map[string][]string` (hash-table, key - string, value - array of strings))  

example:
```json
{
  "type": 1,
  "data": {
    "message": "internal worker error",
    "requeue": true,
    "headers": [
      {
        "test": [
          "1",
          "2",
          "3"
        ]
      }
    ],
    "delay_seconds": 10
  }
}
```

---


#### `ACK` : contains `type` - 2:

example:
```json
{
  "type": 2
}
```

---


#### `NACK` : contains `type` - 3, and `data` field with:
- `message` describing the error or message to print in the logs (type: string).
- `requeue` flag to requeue the job (type: boolean).
- `delay_seconds`: to delay a queue for a provided number of seconds (type: integer).

example:
```json
{
  "type": 3,
  "data": {
    "message": "internal worker error",
    "requeue": true,
    "delay_seconds": 10
  }
}
```


---


#### `REQUEUE` : contains `type` - 4, and `data` field with:
- `message` describing the error or message to print in the logs (type: string).
- `delay_seconds`: to delay a queue for a provided number of seconds (type: integer).
- `headers` - job's headers represented as hashmap with string key and array of strings as a value. (type `map[string][]string` (hash-table, key - string, value - array of strings))

example:
```json
{
  "type": 3,
  "data": {
    "message": "internal worker error",
    "delay_seconds": 10,
    "headers": [
      {
        "test": [
          "1",
          "2",
          "3"
        ]
      }
    ]
  }
}
```
