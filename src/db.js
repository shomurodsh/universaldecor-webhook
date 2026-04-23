// src/db.js
// Подключение к PostgreSQL. Один пул на всё приложение.

import pg from 'pg';

const { Pool } = pg;

// DATABASE_URL — стандартная переменная для подключения к БД.
// Локально формируется из имени пользователя Mac.
// На Railway эта переменная будет задана автоматически.
const connectionString = process.env.DATABASE_URL 
  || `postgresql://${process.env.USER}@localhost:5432/universaldecor`;

export const pool = new Pool({ connectionString });

pool.on('error', (err) => {
  console.error('❌ Неожиданная ошибка PostgreSQL:', err);
});

// Удобный метод для SQL-запросов с логированием времени выполнения
export async function query(text, params) {
  const start = Date.now();
  const res = await pool.query(text, params);
  const duration = Date.now() - start;
  console.log(`📊 SQL (${duration}ms):`, text.substring(0, 60));
  return res;
}