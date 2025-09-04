import fetch from 'node-fetch';
import cheerio from 'cheerio';
import { promises as fs } from 'fs';

// Helper to fetch indicator from Cepea or other sources.
async function fetchCepeaIndicator(url) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`Failed to fetch ${url}`);
  const html = await res.text();
  const $ = cheerio.load(html);
  const firstRow = $('table tbody tr').first();
  const date = firstRow.find('td').eq(0).text().trim();
  let priceStr = firstRow.find('td').eq(1).text().trim();
  // Normalize price string (remove currency symbols, dots and replace comma with dot)
  priceStr = priceStr.replace(/[^0-9,]/g, '').replace(',', '.');
  return { date, price: parseFloat(priceStr) };
}

async function main() {
  // The URLs below are placeholders. Update with actual CEPEA indicator pages.
  // e.g., 'https://www.cepea.esalq.usp.br/br/indicador/arroz.aspx?indicador=CAF%C3%89+AR%C3%81BICA'
  let arabica, robusta, cacao;
  try {
    arabica = await fetchCepeaIndicator('https://www.cepea.esalq.usp.br/en/indicator/coffee_arabica.aspx');
  } catch (err) {
    console.error('Failed to fetch Arabica price:', err.message);
  }
  try {
    robusta = await fetchCepeaIndicator('https://www.cepea.esalq.usp.br/en/indicator/coffee_robusta.aspx');
  } catch (err) {
    console.error('Failed to fetch Robusta price:', err.message);
  }
  try {
    cacao = await fetchCepeaIndicator('https://www.cepea.esalq.usp.br/en/indicator/cocoa.aspx');
  } catch (err) {
    console.error('Failed to fetch Cacao price:', err.message);
  }

  const now = new Date();
  const dateStr = now.toISOString().split('T')[0];

  // Fallback values if scraping failed
  const indicators = {
    cafe: {
      date: arabica?.date || dateStr,
      arabica: arabica?.price || 1250,
      robusta: robusta?.price || 890
    },
    cacau: {
      date: cacao?.date || dateStr,
      bahia: cacao?.price || 820,
      para: cacao?.price ? cacao.price - 30 : 790
    }
  };

  await fs.mkdir('data', { recursive: true });
  await fs.writeFile('data/prices.json', JSON.stringify(indicators, null, 2));
  console.log('Prices updated:', indicators);
}

main().catch(err => console.error(err));
