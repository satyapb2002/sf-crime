import asyncio

from kafka import KafkaConsumer

async def consume(topic_name):
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers= ['localhost:9092'],
        group_id='test'
    )
    
    #consumer.subscribe([topic_name])
    
    for message in consumer:
        if message is None:
            print('Message not found')
        else:
            print(f'{message.value}\n')
                
        await asyncio.sleep(1.0)
                
def run_consumer():
    try:
        asyncio.run(consume('police-calls'))
        
    except KeyboardInterrupt as e:
        print("Shutting down...")
        
if __name__ == '__main__':
    run_consumer()
