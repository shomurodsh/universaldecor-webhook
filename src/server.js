import Fastify from 'fastify';
import formbody from '@fastify/formbody';
import { query, pool } from './db.js';
import { normalizeWebhook } from './normalizer.js';

const fastify = Fastify({ logger: true });
await fastify.register(formbody);

const WEBHOOK_SECRET = process.env.WEBHOOK_SECRET_TOKEN || 'change-me-later';

function parseEntityAndAction(body) {
  if (!body || typeof body !== 'object') return { entity: null, action: null };
  const firstKey = Object.keys(body)[0] || '';
  const entity = firstKey.split('[')[0] || null;
  const match = firstKey.match(/\[([^\]]+)\]/);
  const action = match ? match[1] : null;
  return { entity, action };
}

fastify.get('/', async () => ({
  status: 'ok',
  message: 'Universal Decor webhook server is running',
  timestamp: new Date().toISOString()
}));

fastify.get('/health', async () => {
  try {
    await query('SELECT 1');
    return { status: 'healthy', db: 'connected' };
  } catch (err) {
    return { status: 'unhealthy', db: 'disconnected', error: err.message };
  }
});

fastify.post('/amocrm/receive', async (request, reply) => {
  if (request.query.token !== WEBHOOK_SECRET) {
    request.log.warn({ ip: request.ip }, '❌ Неверный токен');
    return reply.code(401).send({ error: 'Unauthorized' });
  }

  try {
    const { entity, action } = parseEntityAndAction(request.body);

    const result = await query(
      `INSERT INTO amo_webhook_raw (entity, action, payload)
       VALUES ($1, $2, $3)
       RETURNING id`,
      [entity, action, JSON.stringify(request.body)]
    );

 const rawId = result.rows[0].id;
    request.log.info({ id: rawId, entity, action }, '✅ Webhook сохранён');

    // Нормализуем асинхронно — не ждём, отвечаем сразу
    if (entity === 'leads') {
      normalizeWebhook(rawId, request.body).catch(err =>
        request.log.error({ err }, '⚠️ Ошибка нормализации')
      );
    }

    return reply.code(200).send({ success: true, id: rawId });

  } catch (err) {
    request.log.error({ err }, '⚠️ Ошибка БД');
    return reply.code(200).send({ success: false, note: 'stored in logs' });
  }
});

const start = async () => {
  try {
    await query('SELECT 1');
    console.log('✅ База данных подключена');
    await fastify.listen({ port: 3000, host: '0.0.0.0' });
    console.log('✅ Сервер готов: http://localhost:3000');
    console.log('📩 Webhook: POST http://localhost:3000/amocrm/receive?token=' + WEBHOOK_SECRET);
  } catch (err) {
    fastify.log.error(err);
    process.exit(1);
  }
};

process.on('SIGINT', async () => {
  await fastify.close();
  await pool.end();
  process.exit(0);
});

start();