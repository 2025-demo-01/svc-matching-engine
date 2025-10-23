import { sleep } from 'k6';
import exec from 'k6/execution';
import { Kafka } from 'k6/x/kafka';  // xk6-kafka 필요
const topic = __ENV.ORDERS_IN || 'orders.in';
const brokers = (__ENV.KAFKA_BROKERS || 'localhost:9092').split(',');

const producer = new Kafka({ brokers }).producer();

export default function () {
  const msg = JSON.stringify({ order_id: Math.random().toString(36).slice(2), symbol: 'BTCUSDT', side:'buy', price:50000, qty:0.01 });
  producer.produce({ topic, messages: [{ value: msg, key: 'bench' }] });
  sleep(0.01);
}
