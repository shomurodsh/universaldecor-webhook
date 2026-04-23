import 'dotenv/config';
import fs from 'fs';
import pg from 'pg';

const { Pool } = pg;

function createPool() {
  return new Pool({ 
    connectionString: process.env.DATABASE_URL,
    connectionTimeoutMillis: 10000,
    idleTimeoutMillis: 30000,
    max: 3
  });
}

let pool = createPool();

const { access_token } = JSON.parse(fs.readFileSync('tokens.json'));
const DOMAIN = process.env.AMOCRM_DOMAIN;

const FIELD_IDS = {
  client_type:  '927261',
  region:       '927257',
  source:       '927259',
  product_line: '927263',
  destination:  '991053',
  quantity:     '990775',
};

async function fetchLeads(page = 1) {
  const url = `https://${DOMAIN}.amocrm.ru/api/v4/leads?with=contacts&limit=250&page=${page}`;
  const res = await fetch(url, {
    headers: { 'Authorization': `Bearer ${access_token}` }
  });
  if (res.status === 204) return null;
  const data = await res.json();
  return data?._embedded?.leads || null;
}

async function saveLead(lead, retries = 3) {
  const cf = {};
  for (const field of lead.custom_fields_values || []) {
    cf[String(field.field_id)] = field.values?.[0]?.value || null;
  }

  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      await pool.query(
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
          name                = EXCLUDED.name,
          pipeline_id         = EXCLUDED.pipeline_id,
          status_id           = EXCLUDED.status_id,
          price               = EXCLUDED.price,
          responsible_user_id = EXCLUDED.responsible_user_id,
          updated_at          = EXCLUDED.updated_at,
          product_line        = EXCLUDED.product_line,
          source              = EXCLUDED.source,
          region              = EXCLUDED.region,
          client_type         = EXCLUDED.client_type,
          destination         = EXCLUDED.destination,
          quantity            = EXCLUDED.quantity,
          raw_payload         = EXCLUDED.raw_payload,
          synced_at           = NOW()`,
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
      return; // успех
    } catch (err) {
      if (attempt < retries) {
        console.log(`  ⚠️ Попытка ${attempt} неудачна для ${lead.id}, повтор...`);
        // Пересоздаём pool при обрыве соединения
        await pool.end().catch(() => {});
        pool = createPool();
        await new Promise(r => setTimeout(r, 1000 * attempt));
      } else {
        throw err;
      }
    }
  }
}

async function main() {
  console.log('🚀 Начинаем backfill...');
  let page = 1;
  let total = 0;
  let errors = 0;

  while (true) {
    console.log(`📄 Страница ${page}...`);
    
    let leads;
    try {
      leads = await fetchLeads(page);
    } catch (err) {
      console.error('❌ Ошибка загрузки страницы:', err.message);
      break;
    }

    if (!leads || leads.length === 0) {
      console.log('✅ Все страницы загружены');
      break;
    }

    for (const lead of leads) {
      try {
        await saveLead(lead);
        total++;
        if (total % 25 === 0) console.log(`  ✅ Загружено: ${total} сделок`);
      } catch (err) {
        errors++;
        console.error(`  ❌ Ошибка сделки ${lead.id}:`, err.message);
      }
      // Пауза между сделками чтобы не перегружать соединение
      await new Promise(r => setTimeout(r, 50));
    }

    if (leads.length < 250) break;
    page++;
    await new Promise(r => setTimeout(r, 500));
  }

  console.log(`\n🎉 Готово! Загружено: ${total} сделок, ошибок: ${errors}`);
  await pool.end().catch(() => {});
}

main().catch(console.error);
