Different Jobs plugin drivers (aka: amqp, kafka, memory, etc.) might send events to stop or restart a pipeline because it's not possible from inside the driver (Jobs plugin controls the lifetime of its drivers).
This is a list of events that can be sent to the Jobs plugin to stop or restart a pipeline.

`event.Message`:
1. `stop` - Stops the pipeline.
2. `restart` - Restarts the pipeline.

`event.Plugin`:
1. The name of the pipeline to stop or restart.