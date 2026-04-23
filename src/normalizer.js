// src/normalizer.js
// Разбирает сырой payload amoCRM и записывает в таблицу leads.
// amoCRM шлёт ключи вида "leads[update][0][name]" — парсим их в объект.

import { query } from './db.js';

// Превращает плоский объект с ключами "leads[update][0][name]"
// в нормальный объект { id, name, status_id, ... }
function flatToLead(body) {
  const lead = {};

  for (const [key, value] of Object.entries(body)) {
    // Ищем ключи вида leads[любое_действие][0][поле]
    // Например: leads[status][0][id], leads[update][0][name]
    const match = key.match(/^leads\[[^\]]+\]\[0\]\[(.+)\]$/);
    if (match) {
      lead[match[1]] = value;
    }
  }

  return lead;
}

// Основная функция — нормализует одну запись из amo_webhook_raw
export async function normalizeWebhook(rawId, body) {
  try {
    const lead = flatToLead(body);

    // Если нет id сделки — это не сделка (например account-событие)
    if (!lead.id) {
      await query(
        `UPDATE amo_webhook_raw SET processed = true WHERE id = $1`,
        [rawId]
      );
      return null;
    }

    // UPSERT — если сделка уже есть, обновляем; если нет — вставляем
    await query(
      `INSERT INTO leads (
        amo_id, name, pipeline_id, status_id, price,
        responsible_user_id, created_user_id, modified_user_id,
        created_at, updated_at, account_id, raw_payload, synced_at
      ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,
        to_timestamp($9::bigint),
        to_timestamp($10::bigint),
        $11,$12,NOW())
      ON CONFLICT (amo_id) DO UPDATE SET
        name                = EXCLUDED.name,
        pipeline_id         = EXCLUDED.pipeline_id,
        status_id           = EXCLUDED.status_id,
        price               = EXCLUDED.price,
        responsible_user_id = EXCLUDED.responsible_user_id,
        modified_user_id    = EXCLUDED.modified_user_id,
        updated_at          = EXCLUDED.updated_at,
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
        JSON.stringify(body)
      ]
    );

    // Помечаем raw запись как обработанную
    await query(
      `UPDATE amo_webhook_raw SET processed = true WHERE id = $1`,
      [rawId]
    );

    console.log(`✅ Сделка нормализована: id=${lead.id} name="${lead.name}"`);
    return lead.id;

  } catch (err) {
    // Записываем ошибку в raw таблицу
    await query(
      `UPDATE amo_webhook_raw SET error = $1 WHERE id = $2`,
      [err.message, rawId]
    );
    console.error(`❌ Ошибка нормализации raw#${rawId}:`, err.message);
    return null;
  }
}