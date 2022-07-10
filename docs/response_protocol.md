Response protocol used to communicate between worker and RR. When a worker completes its job, it should send a typed
response. The response should contain:

1. `type` field with the message type. Can be treated as enums.
2. `data` field with the dynamic response related to the type.

Types are:

```
0 - NO_ERROR
1 - ERROR
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
