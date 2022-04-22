import time
from kafka import KafkaConsumer, KafkaProducer
from prometheus_client import CollectorRegistry, Gauge, push_to_gateway
import random


# Test config
NUMBER_OF_MESSAGE = 5

# Client config
SERVERS = ["localhost:9092"]
TOPIC = "end2end"

# Producer config
ACK = "all"
LINGER_MS = 0
MAX_BLOCK_MS = 1000000
RECORD_SIZE = 1024

# Consumer config
GROUP_ID = "end2"
AUTO_OFFSET = "lateset"
ENABLE_AUTO_COMMIT = False
FETCH_MAX_WAIT_MS = 0
TIME_OUT = 3000


# Push gateway config
PUSH_PG = True
PG_ADDRESS = "localhost:9091"
PG_JOB_NAME = "kafka_end_to_end_latency"
PG_GROUPING_KEY = "end_to_end_latency"

registry = CollectorRegistry()
g_latency_avg = Gauge("kafka_end_to_end_avg_latency_ms", "Average Kafka end-to-end latency(ms)", registry=registry)
g_latency_p50 = Gauge("kafka_end_to_end_p50_latency_ms", "Percentiles 50th Kafka end-to-end latency(ms)", registry=registry)
g_latency_p99= Gauge("kafka_end_to_end_p99_latency_ms", "Percentiles 99th Kafka end-to-end latency(ms)", registry=registry)
g_latency_p999 = Gauge("kafka_end_to_end_p999_latency_ms", "Percentiles 99.9th Kafka end-to-end latency(ms)", registry=registry)

total_time = 0.0
latencies = {}


producer = KafkaProducer(
                        bootstrap_servers = SERVERS,
                        linger_ms = LINGER_MS,
                        max_block_ms = MAX_BLOCK_MS,
                        acks = ACK
                        )

consumer = KafkaConsumer(
                        bootstrap_servers = SERVERS,
                        group_id = GROUP_ID,
                        auto_offset_reset = AUTO_OFFSET,
                        enable_auto_commit = ENABLE_AUTO_COMMIT,
                        fetch_max_wait_ms = FETCH_MAX_WAIT_MS
                            )
consumer.subscribe(topics=[TOPIC])

def current_milli_time():
    return time.perf_counter() * 1000

def randbytes(size):
    return bytearray(random.getrandbits(8) for _ in range(size))

def finalise():
    consumer.commit()
    consumer.close()
    producer.close()

for i in range(NUMBER_OF_MESSAGE):
    # Initialize msg and time
    message = bytes("Message " + str(i+1),'utf-8')
    begin = current_milli_time()

    # Send a msg
    future = producer.send(TOPIC,message)
    result = future.get(timeout=TIME_OUT)

    # Pull the message
    recordIter = consumer.poll(timeout_ms=TIME_OUT)

    # End time
    end = current_milli_time()
    
    # if it connot get msg.
    if not bool(recordIter):
        finalise()
        err_msg = "poll() timed out before finding a result (timeout:{timeout:d} ms)".format(timeout=TIME_OUT)
        raise RuntimeError(err_msg)

    # if it receives more than 1 msg.
    if len(recordIter) > 2:
        finalise()
        raise RuntimeError("Only one result was expected during this test. We found > 1 msg")
    
    # #  if msg read doesn't match the specific msg.
    read = list(recordIter.values())[0][0].value
    if(read != message):
        finalise()
        raise RuntimeError("The message read [",read,"] did not match the message [",message,"]")

    # Calculate elapsed time
    elapsed = end - begin

    # Report progress
    if i % 10 == 0 and i != 0:
        progress_msg = "(Overview) latency msg {i:d} = {elapsed:.4f} ".format(i=i+1,elapsed=elapsed)
        print(progress_msg)
    total_time += elapsed;
    latencies[i] = elapsed;

    # Commit offset
    consumer.commit()

# Log results
for i in range(NUMBER_OF_MESSAGE) :
    result_msg = "Latency of msg {i:d} = {latency:.4f} ms".format(i=i+1,latency=latencies[i])
    print(result_msg)

# Calculate average latency
avgTime = total_time/(NUMBER_OF_MESSAGE);
print("Avg latency: {avg:.4f} ms".format(avg=avgTime))

# Log different percentiles of latency
sort_latencies = sorted(list(latencies.values()))
p50 = sort_latencies[int(NUMBER_OF_MESSAGE * 0.5)]
p99 = sort_latencies[int(NUMBER_OF_MESSAGE * 0.99)]
p999 = sort_latencies[int(NUMBER_OF_MESSAGE * 0.999)]

print("Percentiles: 50th = {p50:.4f} ms".format(p50=p50))
print("Percentiles: 99th = {p99:.4f} ms".format(p99=p99))
print("Percentiles: 99.9th = {p999:.4f} ms".format(p999=p999))

# Push metrics to push gateway
g_latency_avg.set(avgTime)
g_latency_p50.set(p50)
g_latency_p99.set(p99)
g_latency_p999.set(p999)
push_to_gateway(PG_ADDRESS, job=PG_JOB_NAME, registry=registry)
