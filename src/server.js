import Fastify from 'fastify';
import formbody from '@fastify/formbody';
import { query, pool } from './db.js';
import { normalizeWebhook } from './normalizer.js';
import { generateCSV } from './export.js';

const fastify = Fastify({ logger: true });
await fastify.register(formbody);

const SECRET = process.env.WEBHOOK_SECRET_TOKEN || 'change-me-later';

function parseEntity(body) {
  if (!body || typeof body !== 'object') return { entity: null, action: null };
  const ents = ['leads', 'contacts', 'tasks', 'unsorted', 'customers'];
  const key = Object.keys(body).find(k => ents.some(e => k.startsWith(e))) || '';
  if (!key) return { entity: null, action: null };
  const entity = key.split('[')[0];
  const m = key.match(/\[([^\]]+)\]/);
  return { entity, action: m ? m[1] : null };
}

fastify.get('/', async () => ({
  status: 'ok',
  timestamp: new Date().toISOString()
}));

fastify.get('/health', async () => {
  try {
    await query('SELECT 1');
    return { status: 'healthy', db: 'connected' };
  } catch (err) {
    return { status: 'unhealthy', error: err.message };
  }
});

fastify.get('/api/test-db', async () => {
  try {
    const total = await query('SELECT COUNT(*) FROM leads');
    return { total: total.rows[0].count };
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

fastify.post('/amocrm/receive', async (req, reply) => {
  if (req.query.token !== SECRET) return reply.code(401).send({ error: 'Unauthorized' });
  
  try {
    const { entity, action } = parseEntity(req.body);
    const r = await query(
      'INSERT INTO amo_webhook_raw (entity, action, payload) VALUES ($1,$2,$3) RETURNING id',
      [entity, action, JSON.stringify(req.body)]
    );
    if (entity === 'leads') normalizeWebhook(r.rows[0].id, req.body).catch(console.error);
    return { success: true };
  } catch (err) {
    return { success: false };
  }
});

fastify.get('/admin/backfill', async (req, reply) => {
  if (req.query.token !== SECRET) return reply.code(401).send({ error: 'Unauthorized' });
  runBackfill().catch(console.error);
  return { status: 'started' };
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
      `https://${domain}.amocrm.ru/api/v4/leads?limit=250&page=${page}&with=loss_reason`,
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
      
      await query(
        `INSERT INTO leads (amo_id, name, pipeline_id, status_id, price, 
          responsible_user_id, created_user_id, created_at, updated_at, account_id, 
          product_line, source, region, client_type, destination, quantity, 
          raw_payload, synced_at)
         VALUES ($1,$2,$3,$4,$5,$6,$7,to_timestamp($8),to_timestamp($9),$10,$11,$12,$13,$14,$15,$16,$17,NOW())
         ON CONFLICT (amo_id) DO UPDATE SET 
           name=EXCLUDED.name, status_id=EXCLUDED.status_id, 
           product_line=EXCLUDED.product_line, source=EXCLUDED.source,
           raw_payload=EXCLUDED.raw_payload, synced_at=NOW()`,
        [
          lead.id, lead.name, lead.pipeline_id, lead.status_id, lead.price || 0,
          lead.responsible_user_id, lead.created_by, lead.created_at, lead.updated_at,
          lead.account_id, cf[FIELDS.product_line] || null, cf[FIELDS.source] || null,
          cf[FIELDS.region] || null, cf[FIELDS.client_type] || null,
          cf[FIELDS.destination] || null, cf[FIELDS.quantity] || null,
          JSON.stringify(lead)
        ]
      );
      total++;
    }
    
    console.log(`Backfill page ${page}: ${total}`);
    if (leads.length < 250) break;
    page++;
    await new Promise(r => setTimeout(r, 300));
  }
  
  console.log(`Backfill done: ${total} leads`);
}

try {
  await query('SELECT 1');
  console.log('DB connected');
  await fastify.listen({ port: process.env.PORT || 3000, host: '0.0.0.0' });
  console.log('Server ready');
} catch (err) {
  console.error(err);
  process.exit(1);
}

process.on('SIGINT', async () => {
  await fastify.close();
  await pool.end();
  process.exit(0);
});
