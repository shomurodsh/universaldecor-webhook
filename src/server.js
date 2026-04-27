import Fastify from 'fastify';
import formbody from '@fastify/formbody';
import { query, pool } from './db.js';
import { normalizeWebhook } from './normalizer.js';
import { generateCSV } from './export.js';

const fastify = Fastify({ logger: true });
await fastify.register(formbody);

const WEBHOOK_SECRET = process.env.WEBHOOK_SECRET_TOKEN || 'change-me-later';

function parseEntityAndAction(body) {
  if (!body || typeof body !== 'object') return { entity: null, action: null };

  // Ищем ключ который начинается с "leads", "contacts", "tasks" и т.д.
  // НЕ "account" — это метаданные, не сущность
  const entities = ['leads', 'contacts', 'tasks', 'unsorted', 'customers'];
  
  const firstKey = Object.keys(body).find(key => 
    entities.some(e => key.startsWith(e))
  ) || '';

  if (!firstKey) return { entity: null, action: null };

  const entity = firstKey.split('[')[0];
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

fastify.get('/export', async (req, reply) => {
  try {
    const result = await query(`
      SELECT amo_id, name, price, pipeline_id, status_id, responsible_user_id,
        product_line, source, region, client_type, destination, quantity,
        created_at, updated_at
      FROM leads ORDER BY created_at DESC
    `);
    
    const headers = ['ID', 'Название', 'Цена', 'Воронка', 'Статус', 'Менеджер ID', 'Продукт', 'Источник', 'Регион', 'Тип клиента', 'Куда', 'Количество', 'Создано', 'Обновлено'];
    
    let csv = '\uFEFF' + headers.join(',') + '\n';
    
    for (const row of result.rows) {
      const values = [
        row.amo_id,
        `"${(row.name || '').replace(/"/g, '""')}"`,
        row.price || 0,
        row.pipeline_id || '',
        row.status_id || '',
        row.responsible_user_id || '',
        `"${row.product_line || ''}"`,
        `"${row.source || ''}"`,
        `"${row.region || ''}"`,
        `"${row.client_type || ''}"`,
        `"${row.destination || ''}"`,
        `"${row.quantity || ''}"`,
        row.created_at ? new Date(row.created_at).toISOString() : '',
        row.updated_at ? new Date(row.updated_at).toISOString() : ''
      ];
      csv += values.join(',') + '\n';
    }
    
    reply
      .type('text/csv; charset=utf-8')
      .header('Content-Disposition', 'attachment; filename="universaldecor_leads.csv"')
      .send(csv);
  } catch (err) {
    return reply.code(500).send({ error: err.message });
  }
});

fastify.get('/api/test-db', async () => {
  try {
    const total = await query('SELECT COUNT(*) FROM leads');
    const sample = await query('SELECT amo_id, name, product_line, source FROM leads LIMIT 5');
    return {
      total: total.rows[0].count,
      sample: sample.rows
    };
  } catch (err) {
    return { error: err.message };
  }
});

fastify.get('/export', async (req, reply) => {
  try {
    const csv = await generateCSV();
    reply
      .type('text/csv; charset=utf-8')
      .header('Content-Disposition', 'attachment; filename="universaldecor_export.csv"')
      .send(csv);
  } catch (err) {
    return reply.code(500).send({ error: err.message });
  }
});

fastify.post('/amocrm/receive', async (request, reply) => {
  if (request.query.token !== WEBHOOK_SECRET) {
    request.log.warn({ ip: request.ip }, '❌ Неверный токен');
    return reply.code(401).send({ error: 'Unauthorized' });
  }

  try {
    const { entity, action } = parseEntityAndAction(request.body);
    console.log('🔍 DEBUG entity:', entity, 'action:', action);

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

// Временный endpoint для backfill — запускается один раз
fastify.get('/admin/backfill', async (request, reply) => {
  if (request.query.token !== WEBHOOK_SECRET) {
    return reply.code(401).send({ error: 'Unauthorized' });
  }

  // Запускаем в фоне
  runBackfill().catch(console.error);
  return { status: 'started', message: 'Backfill запущен в фоне' };
});

async function runBackfill() {
  const FIELDS = { 
    product_line: '927263', 
    source: '927259', 
    region: '927257', 
    client_type: '927261', 
    destination: '991053', 
    quantity: '990775' 
  };
  
  const token = process.env.AMOCRM_LONG_TOKEN;
  const domain = process.env.AMOCRM_DOMAIN || 'universaldecor';
  
  let page = 1, total = 0;
  
  while (true) {
    const res = await fetch(
      `https://${domain}.amocrm.ru/api/v4/leads?limit=250&page=${page}&with=contacts,loss_reason`,
      { headers: { 'Authorization': `Bearer ${token}` } }
    );
    
    if (res.status === 204) break;
    
    const data = await res.json();
    const leads = data?._embedded?.leads;
    
    if (!leads || leads.length === 0) break;
    
    for (const lead of leads) {
      const cf = {};
      for (const f of lead.custom_fields_values || []) {
        cf[String(f.field_id)] = f.values?.[0]?.value || null;
      }
      
      const contacts = lead._embedded?.contacts || [];
      const tags = (lead._embedded?.tags || []).map(t => t.name).join(', ');
      const lossReason = lead._embedded?.loss_reason?.[0]?.name || null;
      
      let phone = null, email = null;
      if (contacts.length > 0) {
        const mainContactId = contacts.find(c => c.is_main)?.id || contacts[0].id;
        try {
          const contactRes = await fetch(
            `https://${domain}.amocrm.ru/api/v4/contacts/${mainContactId}`,
            { headers: { 'Authorization': `Bearer ${token}` } }
          );
          const contactData = await contactRes.json();
          for (const f of contactData.custom_fields_values || []) {
            if (f.field_code === 'PHONE') phone = f.values?.[0]?.value || null;
            if (f.field_code === 'EMAIL') email = f.values?.[0]?.value || null;
          }
        } catch (e) {}
      }
      
      await query(
        `INSERT INTO leads (amo_id, name, pipeline_id, status_id, price, 
          responsible_user_id, created_user_id, created_at, updated_at, account_id, 
          product_line, source, region, client_type, destination, quantity, 
          contacts, tags, loss_reason, closed_at, closest_task_at, phone, email,
          raw_payload, synced_at)
         VALUES ($1,$2,$3,$4,$5,$6,$7,to_timestamp($8),to_timestamp($9),$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,
           ${lead.closed_at ? 'to_timestamp($20)' : 'NULL'},
           ${lead.closest_task_at ? `to_timestamp(${lead.closed_at ? '$21' : '$20'})` : 'NULL'},
           $${20 + (lead.closed_at ? 1 : 0) + (lead.closest_task_at ? 1 : 0)},
           $${21 + (lead.closed_at ? 1 : 0) + (lead.closest_task_at ? 1 : 0)},
           $${22 + (lead.closed_at ? 1 : 0) + (lead.closest_task_at ? 1 : 0)},
           NOW())
         ON CONFLICT (amo_id) DO UPDATE SET 
           name=EXCLUDED.name, status_id=EXCLUDED.status_id, 
           product_line=EXCLUDED.product_line, source=EXCLUDED.source, 
           contacts=EXCLUDED.contacts, tags=EXCLUDED.tags,
           loss_reason=EXCLUDED.loss_reason, phone=EXCLUDED.phone, email=EXCLUDED.email,
           synced_at=NOW()`,
        [
          lead.id, lead.name, lead.pipeline_id, lead.status_id, lead.price || 0,
          lead.responsible_user_id, lead.created_by, lead.created_at, lead.updated_at,
          lead.account_id, cf[FIELDS.product_line] || null, cf[FIELDS.source] || null,
          cf[FIELDS.region] || null, cf[FIELDS.client_type] || null,
          cf[FIELDS.destination] || null, cf[FIELDS.quantity] || null,
          JSON.stringify(contacts), tags, lossReason,
          ...(lead.closed_at ? [lead.closed_at] : []),
          ...(lead.closest_task_at ? [lead.closest_task_at] : []),
          phone, email, JSON.stringify(lead)
        ]
      );
      
      total++;
    }
    
    console.log(`Backfill page ${page}: ${total}`);
    
    if (leads.length < 250) break;
    page++;
    await new Promise(r => setTimeout(r, 500));
  }
  
  console.log(`🎉 Backfill done: ${total} leads`);
}
  const token = process.env.AMOCRM_LONG_TOKEN;
  const domain = process.env.AMOCRM_DOMAIN;
  let page = 1;
  let total = 0;

  console.log('🚀 Backfill started...');

  while (true) {
    const res = await fetch(
      `https://${domain}.amocrm.ru/api/v4/leads?limit=250&page=${page}`,
      { headers: { 'Authorization': `Bearer ${token}` } }
    );

    if (res.status === 204) break;
    const data = await res.json();
    const leads = data?._embedded?.leads;
    if (!leads || leads.length === 0) break;

    for (const lead of leads) {
      const cf = {};
      for (const field of lead.custom_fields_values || []) {
        cf[String(field.field_id)] = field.values?.[0]?.value || null;
      }

      await query(
        `INSERT INTO leads (
          amo_id, name, pipeline_id, status_id, price,
          responsible_user_id, created_user_id,
          created_at, updated_at, account_id,
          product_line, source, region, client_type, destination, quantity,
          raw_payload, synced_at
        ) VALUES ($1,$2,$3,$4,$5,$6,$7,
          to_timestamp($8), to_timestamp($9),
          $10,$11,$12,$13,$14,$15,$16,$17,NOW())
        ON CONFLICT (amo_id) DO UPDATE SET
          name = EXCLUDED.name, status_id = EXCLUDED.status_id,
          product_line = EXCLUDED.product_line, source = EXCLUDED.source,
          region = EXCLUDED.region, synced_at = NOW()`,
        [
          lead.id, lead.name, lead.pipeline_id, lead.status_id, lead.price || 0,
          lead.responsible_user_id, lead.created_by,
          lead.created_at, lead.updated_at, lead.account_id,
          cf[FIELD_IDS.product_line] || null,
          cf[FIELD_IDS.source] || null,
          cf[FIELD_IDS.region] || null,
          cf[FIELD_IDS.client_type] || null,
          cf[FIELD_IDS.destination] || null,
          cf[FIELD_IDS.quantity] || null,
          JSON.stringify(lead)
        ]
      );
      total++;
    }

    console.log(`✅ Backfill page ${page}: total ${total}`);
    if (leads.length < 250) break;
    page++;
    await new Promise(r => setTimeout(r, 300));
  }

  console.log(`🎉 Backfill done: ${total} leads`);

start();