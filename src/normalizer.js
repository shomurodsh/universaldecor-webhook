// src/normalizer.js
import { query } from './db.js';

// ID кастомных полей из твоего amoCRM
const FIELD_IDS = {
  client_type:  '927261',  // Mijoz turi
  region:       '927257',  // Viloyat
  source:       '927259',  // Istochnik
  product_line: '927263',  // Mahsulot turi
  destination:  '991053',  // Qayerga olmoqchi
  quantity:     '990775',  // Maxsulot soni / metr
};

// Парсим плоский payload в объект сделки
function flatToLead(body) {
  const lead = {};
  const customFields = {};

  for (const [key, value] of Object.entries(body)) {
    // Пропускаем account поля
    if (key.startsWith('account[')) continue;

    // Кастомные поля: leads[action][0][custom_fields][N][id]
    const cfIdMatch = key.match(/\[custom_fields\]\[(\d+)\]\[id\]$/);
    if (cfIdMatch) {
      const idx = cfIdMatch[1];
      if (!customFields[idx]) customFields[idx] = {};
      customFields[idx].id = value;
      continue;
    }

    // Кастомные поля: leads[action][0][custom_fields][N][values][0][value]
    const cfValMatch = key.match(/\[custom_fields\]\[(\d+)\]\[values\]\[0\]\[value\]$/);
    if (cfValMatch) {
      const idx = cfValMatch[1];
      if (!customFields[idx]) customFields[idx] = {};
      customFields[idx].value = value;
      continue;
    }

    // Основные поля: leads[action][0][fieldname]
    const mainMatch = key.match(/^leads\[[^\]]+\]\[0\]\[([^\]]+)\]$/);
    if (mainMatch) {
      lead[mainMatch[1]] = value;
    }
  }

  // Маппим по ID поля
  const cf = {};
  for (const field of Object.values(customFields)) {
    if (field.id && field.value !== undefined) {
      cf[field.id] = field.value;
    }
  }

  console.log('🔍 DEBUG cf:', JSON.stringify(cf));
  return { lead, cf };
}

export async function normalizeWebhook(rawId, body) {
  try {
    const { lead, cf } = flatToLead(body);

    if (!lead.id) {
      await query(`UPDATE amo_webhook_raw SET processed = true WHERE id = $1`, [rawId]);
      return null;
    }

    await query(
      `INSERT INTO leads (
        amo_id, name, pipeline_id, status_id, price,
        responsible_user_id, created_user_id, modified_user_id,
        created_at, updated_at, account_id,
        product_line, source, region, client_type, destination, quantity,
        raw_payload, synced_at
      ) VALUES (
        $1,$2,$3,$4,$5,$6,$7,$8,
        to_timestamp($9::bigint), to_timestamp($10::bigint),
        $11,$12,$13,$14,$15,$16,$17,$18,NOW()
      )
      ON CONFLICT (amo_id) DO UPDATE SET
        name                = EXCLUDED.name,
        pipeline_id         = EXCLUDED.pipeline_id,
        status_id           = EXCLUDED.status_id,
        price               = EXCLUDED.price,
        responsible_user_id = EXCLUDED.responsible_user_id,
        modified_user_id    = EXCLUDED.modified_user_id,
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
        lead.id,
        lead.name || null,
        lead.pipeline_id || null,
        lead.status_id || null,
        lead.price || 0,
        lead.responsible_user_id || null,
        lead.created_user_id || null,
        lead.modified_user_id || null,
        lead.created_at || null,
        lead.updated_at || null,
        body['account[id]'] || null,
        cf[FIELD_IDS.product_line] || null,
        cf[FIELD_IDS.source] || null,
        cf[FIELD_IDS.region] || null,
        cf[FIELD_IDS.client_type] || null,
        cf[FIELD_IDS.destination] || null,
        cf[FIELD_IDS.quantity] || null,
        JSON.stringify(body)
      ]
    );

    await query(`UPDATE amo_webhook_raw SET processed = true WHERE id = $1`, [rawId]);
console.log('🔍 DEBUG cf:', JSON.stringify(cf));
    console.log('🔍 DEBUG lead:', JSON.stringify(lead));
    console.log(`✅ Сделка: id=${lead.id} name="${lead.name}" product="${cf[FIELD_IDS.product_line]}"`);
    return lead.id;

  } catch (err) {
    await query(`UPDATE amo_webhook_raw SET error = $1 WHERE id = $2`, [err.message, rawId]);
    console.error(`❌ Ошибка raw#${rawId}:`, err.message);
    return null;
  }
}