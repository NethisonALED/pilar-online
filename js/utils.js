// --- CONFIGURAÇÃO E INICIALIZAÇÃO DO SUPABASE ---
// Esta instância é exportada para ser usada em toda a aplicação.
// Importa e configura o dotenv
import { SUPABASE_URL, SUPABASE_ANON_KEY } from './config.js';
export const supabase = window.supabase.createClient(SUPABASE_URL, SUPABASE_ANON_KEY);


// --- FUNÇÕES UTILITÁRIAS EXPORTADAS ---

/**
 * Converte um objeto de arquivo (File) para uma string Base64.
 * @param {File} file - O arquivo a ser convertido.
 * @returns {Promise<string>} Uma promessa que resolve com a URL de dados Base64.
 */
export const fileToBase64 = (file) => new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.readAsDataURL(file);
    reader.onload = () => resolve(reader.result);
    reader.onerror = error => reject(error);
});

/**
 * Converte um array de objetos JSON para uma Data URL de um arquivo XLSX (Excel).
 * @param {Array<Object>} jsonData - Os dados a serem convertidos.
 * @returns {string} A URL de dados para o arquivo XLSX.
 */
export const jsonToXLSXDataURL = (jsonData) => {
    const ws = XLSX.utils.json_to_sheet(jsonData);
    const wb = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(wb, ws, "Sysled Import");
    const wbout = XLSX.write(wb, { bookType: 'xlsx', type: 'base64' });
    return "data:application/vnd.openxmlformats-officedocument.spreadsheetml.sheet;base64," + wbout;
};

/**
 * Converte um número ou string vindo da API (com vírgula decimal) para um número.
 * @param {*} value - O valor a ser convertido.
 * @returns {number} O valor convertido para número.
 */
export const parseApiNumber = (value) => {
    if (value === null || value === undefined) return 0;
    return Number(String(value).replace(',', '.')) || 0;
};

/**
 * Converte uma string de moeda formatada (ex: "R$ 1.234,56") para um número.
 * @param {*} value - A string de moeda.
 * @returns {number} O valor numérico.
 */
export const parseCurrency = (value) => {
    if (typeof value === 'number') return value;
    if (typeof value !== 'string' || value === null) return 0;
    return parseFloat(String(value).replace(/R\$\s?/, '').replace(/\./g, '').replace(',', '.')) || 0;
};

/**
 * Formata um número como uma string de moeda brasileira (BRL).
 * @param {number|string} value - O valor a ser formatado.
 * @returns {string} A string formatada (ex: "R$ 1.234,56").
 */
export const formatCurrency = (value) => {
    const number = parseCurrency(value);
    return number.toLocaleString('pt-BR', { style: 'currency', currency: 'BRL' });
};

/**
 * Formata uma string de data do tipo ISO (YYYY-MM-DDTHH:mm:ss) para o formato brasileiro (DD/MM/YYYY).
 * @param {string} dateString - A string de data.
 * @returns {string} A data formatada.
 */
export const formatApiDateToBR = (dateString) => {
     if (!dateString || typeof dateString !== 'string') return '';
     const datePart = dateString.split('T')[0];
     const parts = datePart.split('-');
     if (parts.length !== 3) return dateString; 
     const [year, month, day] = parts;
     return `${day}/${month}/${year}`;
};

/**
 * Formata um número vindo da API como uma string com formatação brasileira (ponto como milhar, vírgula como decimal).
 * @param {*} value - O valor a ser formatado.
 * @returns {string} A string numérica formatada.
 */
export const formatApiNumberToBR = (value) => {
    if (value === null || value === undefined || value === '') return '';
    const number = parseApiNumber(value);
    if (isNaN(number)) return value;
    return number.toLocaleString('pt-BR', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
};
