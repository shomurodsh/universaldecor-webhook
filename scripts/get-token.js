import 'dotenv/config';
import fs from 'fs';

const { AMOCRM_DOMAIN, AMOCRM_LONG_TOKEN } = process.env;

console.log('🔑 Проверяем долгосрочный токен...');
console.log('Domain:', AMOCRM_DOMAIN);
console.log('Token starts with:', AMOCRM_LONG_TOKEN?.substring(0, 20) + '...');

const response = await fetch(
  `https://${AMOCRM_DOMAIN}.amocrm.ru/api/v4/account`,
  {
    headers: { 
      'Authorization': `Bearer ${AMOCRM_LONG_TOKEN}`,
      'Content-Type': 'application/json'
    }
  }
);

const data = await response.json();
console.log('Ответ:', JSON.stringify(data, null, 2));

if (data.id) {
  console.log('✅ Токен работает! Аккаунт:', data.name);
  fs.writeFileSync('tokens.json', JSON.stringify({ access_token: AMOCRM_LONG_TOKEN }, null, 2));
  console.log('✅ Сохранено в tokens.json');
} else {
  console.error('❌ Ошибка токена');
}
