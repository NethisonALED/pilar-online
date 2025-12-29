import { supabase, parseCurrency, formatCurrency, fileToBase64, jsonToXLSXDataURL, formatApiDateToBR, formatApiNumberToBR, parseApiNumber } from './utils.js';
import { getSecureContext } from './secure-context.js';
import { initializeEventListeners } from './events.js';
class RelacionamentoApp {

        
    
    constructor() {
        // Estado da aplicação
        this.arquitetos = [];
        this.pontuacoes = {};
        this.pagamentos = {};
        this.resgates = []; // NOVO: Para armazenar resgates separadamente
        this.importedFiles = {};
        this.comissoesManuais = [];
        this.actionLogs = []; // Para armazenar os logs de eventos
        this.tempRTData = [];
        this.carteira = []; // Dados da carteira
        this.tempCarteiraData = []; // Dados temporários da importação da carteira
        this.users = []; // Para gerenciar permissões
        this.tempArquitetoData = [];
        this.eligibleForPayment = [];
        this.currentUserEmail = ''; // Para armazenar o email do usuário logado
        this.currentUserId = null; // Para armazenar o ID do usuário logado
        this.minPaymentValue = 300; // Valor mínimo para pagamento (padrão)
        this.carteiraSortDirection = 'desc'
        this.carteiraSortColumn = 'tempo_sem_envio'
        // Flags para funcionalidades condicionais baseadas no schema do DB
        this.schemaHasRtAcumulado = false;
        this.schemaHasRtTotalPago = false;
        
        // Dados da API Sysled
        this.sysledData = [];
        this.sysledFilteredData = [];
        this.isSysledImport = false;
        this.pendingImportData = null; // Para armazenar dados de importação pendentes
        
        // Estado de ordenação da tabela de arquitetos
        this.sortColumn = 'nome';
        this.sortDirection = 'asc';

        // Inicializa credenciais seguras
        const secureData = getSecureContext();
        this.sysledApiUrl = 'https://integration.sysled.com.br/n8n/api/?v_crm_oportunidades_propostas_up180dd=null';
        this.sysledAuthToken = 'e4b6f9082f1b8a1f37ad5b56e637f3ec719ec8f0b6acdd093972f9c5bb29b9ed';

        this.init();
    }

    /**
     * Inicializa a aplicação, carregando dados e configurando os event listeners.
     */
    async init() {
        const { data: { session } } = await supabase.auth.getSession();
        if (session) {
            this.currentUserEmail = session.user.email;
            this.currentUserId = session.user.id;
        }
        await this.loadData();
        initializeEventListeners(this);
        this.renderAll();
    }
    
    /**
     * Carrega todos os dados iniciais do Supabase.
     */
    async loadData() {
        console.log("Carregando dados do Supabase...");
        
        const [arqRes, pagRes, filesRes, comissoesRes, logsRes, usersRes, carteiraRes] = await Promise.all([
            supabase.from('arquitetos').select('*'),
            supabase.from('pagamentos').select('*'),
            supabase.from('arquivos_importados').select('*'),
            supabase.from('comissoes_manuais').select('*').order('created_at', { ascending: false }),
            supabase.from('action_logs').select('*').order('when_did', { ascending: false }), // Carrega os logs
            supabase.from('profiles').select('id, email, role'), // Carrega usuários e suas roles
            supabase.from('carteira').select('*') // Carrega dados da carteira
        ]);

        if (arqRes.error) console.error('Erro ao carregar arquitetos:', arqRes.error);
        else {
            this.arquitetos = arqRes.data || [];
            if (this.arquitetos.length > 0) {
                const first = this.arquitetos[0];
                this.schemaHasRtAcumulado = first.hasOwnProperty('rt_acumulado');
                this.schemaHasRtTotalPago = first.hasOwnProperty('rt_total_pago');
            }
            this.pontuacoes = this.arquitetos.reduce((acc, arq) => ({...acc, [arq.id]: arq.pontos || 0}), {});
        }

        if (pagRes.error) console.error('Erro ao carregar pagamentos:', pagRes.error);
        else {
            this.pagamentos = {}; // Reset
            this.resgates = [];   // Reset
            (pagRes.data || []).forEach(p => {
                // A coluna 'form_pagamento' com valor 2 indica um resgate
                if (p.form_pagamento === 2) {
                    this.resgates.push(p);
                } else { // Pagamentos normais (form_pagamento === 1 ou null/undefined)
                    const dateKey = new Date(p.data_geracao + 'T00:00:00Z').toLocaleDateString('pt-BR');
                    if (!this.pagamentos[dateKey]) this.pagamentos[dateKey] = [];
                    this.pagamentos[dateKey].push(p);
                }
            });
        }

        if (filesRes.error) console.error('Erro ao carregar arquivos:', filesRes.error);
        else {
            this.importedFiles = (filesRes.data || []).reduce((acc, f) => {
                const dateKey = new Date(f.data_importacao + 'T00:00:00Z').toLocaleDateString('pt-BR');
                acc[dateKey] = { name: f.name, dataUrl: f.dataUrl, id: f.id };
                return acc;
            }, {});
        }
        
        if (comissoesRes.error) console.error('Erro ao carregar comissões manuais:', comissoesRes.error);
        else this.comissoesManuais = comissoesRes.data || [];

        // Processa os logs de eventos
        if (logsRes.error) console.error('Erro ao carregar logs de eventos:', logsRes.error);
        else this.actionLogs = logsRes.data || [];

        // Carrega todos os perfis de usuário (para aba de permissões e para a foto do usuário logado)
        const { data: profilesData, error: profilesError } = await supabase.from('profiles').select('id, email, role, avatar_url');
        if (profilesError) {
            console.error('Erro ao carregar perfis de usuário:', profilesError);
        } else {
            this.users = profilesData || [];
        }

        if (carteiraRes.error) console.error('Erro ao carregar carteira:', carteiraRes.error);
        else this.carteira = carteiraRes.data || [];

        console.log("Dados carregados.");
    }
        
    /**
     * Renderiza ou atualiza todos os componentes visuais da aplicação.
     */
    renderAll() {
        this.renderArquitetosTable();
        this.renderRankingTable();
        this.populateArquitetoSelect();
        this.renderPagamentos();
        this.renderResgates(); // NOVO: Renderiza a tabela de resgates
        this.renderArquivosImportados();
        this.renderHistoricoManual();
        this.renderResultados();
        this.renderEventosLog(); // Renderiza a tabela de logs
        this.renderPermissionsTab(); // Renderiza a aba de permissões
        this.checkPaymentFeature();
        this.renderCarteiraTab(); // Renderiza a aba Carteira
        console.log("Todos os componentes foram renderizados.");
    }
    
    // --- MÉTODOS DE RENDERIZAÇÃO E UI ---
    
    renderArquitetosTable() {
        const container = document.getElementById('arquitetos-table-container');
        if (!container) return;
        const filter = document.getElementById('arquiteto-search-input').value.toLowerCase();
        let filteredArquitetos = this.arquitetos.filter(a => 
            (a.id || '').toString().toLowerCase().includes(filter) || 
            (a.nome || '').toLowerCase().includes(filter)
        );

        filteredArquitetos.sort((a, b) => {
            const key = this.sortColumn;
            const dir = this.sortDirection === 'asc' ? 1 : -1;
            let valA = a[key] ?? '';
            let valB = b[key] ?? '';
            if (['valorVendasTotal', 'salesCount', 'rt_acumulado', 'rt_total_pago', 'pontos'].includes(key)) {
                valA = parseFloat(valA) || 0;
                valB = parseFloat(valB) || 0;
            }
            if (typeof valA === 'string') return valA.localeCompare(valB) * dir;
            if (valA < valB) return -1 * dir;
            if (valA > valB) return 1 * dir;
            return 0;
        });

        const controlsHtml = `
            <div class="flex justify-end items-center gap-2 mb-2 px-2">
                <label for="min-payment-value" class="text-sm text-gray-300">Mínimo para Pagamento (R$):</label>
                <input type="number" id="min-payment-value" class="glass-input py-1 px-2 rounded w-28 text-right bg-background-dark border border-white/10 text-white focus:border-primary focus:ring-1 focus:ring-primary outline-none transition-all" value="${this.minPaymentValue}" step="0.1">
            </div>
        `;

        if (filteredArquitetos.length === 0) {
            container.innerHTML = controlsHtml + `<p class="text-center text-gray-400 py-4">Nenhum arquiteto encontrado.</p>`;
            return;
        }

        const getSortIcon = (column) => {
            if (this.sortColumn !== column) return '<i class="fas fa-sort text-gray-300 ml-1"></i>';
            return this.sortDirection === 'asc' ? '<i class="fas fa-sort-up text-primary ml-1"></i>' : '<i class="fas fa-sort-down text-primary ml-1"></i>';
        };

        const headerRtAcumulado = this.schemaHasRtAcumulado ? `<th class="sortable-header cursor-pointer" data-sort="rt_acumulado">RT Acumulado ${getSortIcon('rt_acumulado')}</th>` : '';
        const headerRtTotal = this.schemaHasRtTotalPago ? `<th class="sortable-header cursor-pointer" data-sort="rt_total_pago">Total Pago ${getSortIcon('rt_total_pago')}</th>` : '';
        const headerRow = `<tr>
                                <th class="sortable-header cursor-pointer" data-sort="id">ID ${getSortIcon('id')}</th>
                                <th class="sortable-header cursor-pointer" data-sort="nome">Nome ${getSortIcon('nome')}</th>
                                <th class="sortable-header cursor-pointer text-center" data-sort="salesCount">Vendas ${getSortIcon('salesCount')}</th>
                                <th class="sortable-header cursor-pointer text-right" data-sort="valorVendasTotal">Valor Vendas ${getSortIcon('valorVendasTotal')}</th>
                                ${headerRtAcumulado}${headerRtTotal}
                                <th class="text-center">Ações</th></tr>`;

        const rows = filteredArquitetos.map(a => {
            let cellRtAcumulado = '';
            if (this.schemaHasRtAcumulado) {
                const rtAcumulado = a.rt_acumulado || 0;
                cellRtAcumulado = `<td class="text-right font-semibold ${rtAcumulado >= this.minPaymentValue ? 'text-primary' : ''}">${formatCurrency(rtAcumulado)}</td>`;
            }
            const cellRtTotal = this.schemaHasRtTotalPago ? `<td class="text-right">${formatCurrency(a.rt_total_pago || 0)}</td>` : '';
            return `<tr>
                <td><a href="#" class="id-link text-primary/80 hover:text-primary font-semibold" data-id="${a.id}">${a.id}</a></td>
                <td>${a.nome}</td>
                <td class="text-center">${a.salesCount || 0}</td>
                <td class="text-right">${formatCurrency(a.valorVendasTotal || 0)}</td>
                ${cellRtAcumulado}${cellRtTotal}
                <td class="text-center">
                    <button class="add-value-btn text-green-400 hover:text-green-300" title="Adicionar Valor Manual" data-id="${a.id}"><span class="material-symbols-outlined">add_circle</span></button>
                    <button class="edit-btn text-blue-400 hover:text-blue-300 ml-2" title="Editar" data-id="${a.id}"><span class="material-symbols-outlined">edit</span></button>
                    <button class="delete-btn text-red-500 hover:text-red-400 ml-2" title="Apagar" data-id="${a.id}"><span class="material-symbols-outlined">delete</span></button>
                </td></tr>`;
        }).join('');
        
        container.innerHTML = controlsHtml + `<div class="max-h-[65vh] overflow-y-auto"><table class="w-full"><thead>${headerRow}</thead><tbody>${rows}</tbody></table></div>`;
    }

    renderPagamentos(filter = '') {
        const container = document.getElementById('pagamentos-container');
        if (!container) return;
        container.innerHTML = '';
        const dates = Object.keys(this.pagamentos).sort((a,b) => new Date(b.split('/').reverse().join('-')) - new Date(a.split('/').reverse().join('-')));
        if (dates.length === 0) {
            container.innerHTML = `<div class="glass-card rounded-lg p-6 text-center text-gray-400">Nenhum pagamento foi gerado ainda.</div>`; return;
        }

        let hasResults = false;
        dates.forEach(date => {
            let pagamentosDoDia = this.pagamentos[date].filter(p => !filter || (p.id_parceiro && p.id_parceiro.toString().includes(filter)));
            if (pagamentosDoDia.length > 0) {
                hasResults = true;
                const rowsHtml = pagamentosDoDia.map(p => {
                    const hasComprovante = p.comprovante && p.comprovante.url;
                    return `<tr>
                                <td>${p.id_parceiro}</td>
                                <td>${p.parceiro}</td>
                                <td>${p.consultor || 'N/A'}</td>
                                <td class="text-right font-semibold">${formatCurrency(p.rt_valor)}<button class="edit-rt-btn text-blue-400 hover:text-blue-300 ml-2" title="Editar Valor RT" data-id="${p.id}"><span class="material-symbols-outlined text-base align-middle">edit</span></button></td>
                                <td class="text-center"><input type="checkbox" class="pagamento-status h-5 w-5 rounded bg-background-dark border-white/20 text-primary focus:ring-primary" data-id="${p.id}" ${p.pago ? 'checked' : ''}></td>
                                <td><div class="flex items-center gap-2"><label for="comprovante-input-${p.id}" class="file-input-label bg-white/10 hover:bg-white/20 text-xs py-1 px-3 !font-medium whitespace-nowrap">Anexar</label><input type="file" id="comprovante-input-${p.id}" class="comprovante-input file-input" data-id="${p.id}"><span class="file-status-text text-xs ${hasComprovante ? 'text-green-400 font-semibold' : 'text-gray-400'}">${hasComprovante ? 'Comprovante anexado' : 'Nenhum arquivo'}</span></div></td>
                                <td class="text-center"><button class="view-comprovante-btn text-primary/80 hover:text-primary font-semibold" data-id="${p.id}" ${!hasComprovante ? 'disabled' : ''} style="${!hasComprovante ? 'opacity: 0.5; cursor: not-allowed;' : ''}">Ver</button></td>
                            </tr>`;
                }).join('');
                container.innerHTML += `<div class="payment-group-card"><div class="flex flex-wrap justify-between items-center mb-4 gap-4"><h2 class="text-xl font-semibold">Pagamentos Gerados em ${date}</h2><div class="flex items-center gap-2"><button class="gerar-relatorio-btn btn-modal !py-1 !px-3 !text-xs bg-blue-500/80 hover:bg-blue-500" data-date="${date}">Gerar Relatório</button><button class="download-xlsx-btn btn-modal !py-1 !px-3 !text-xs bg-green-500/80 hover:bg-green-500" data-date="${date}">Baixar XLSX</button><button class="delete-pagamentos-btn btn-modal !py-1 !px-3 !text-xs bg-red-600/80 hover:bg-red-600" data-date="${date}">Excluir Lote</button></div></div><div class="overflow-x-auto"><table><thead><tr><th>ID Parceiro</th><th>Parceiro</th><th>Consultor</th><th class="text-right">Valor RT</th><th class="text-center">Pago</th><th>Anexar Comprovante</th><th class="text-center">Ver</th></tr></thead><tbody>${rowsHtml}</tbody></table></div></div>`;
            }
        });
        if (!hasResults && filter) container.innerHTML = `<div class="glass-card rounded-lg p-6 text-center text-gray-400">Nenhum pagamento encontrado para o ID informado.</div>`;
    }

    /**
     * NOVO: Renderiza a tabela unificada de resgates.
     */
    renderResgates(filter = '') {
        const container = document.getElementById('resgates-container');
        if (!container) return;

        let filteredResgates = this.resgates.filter(p => !filter || (p.id_parceiro && p.id_parceiro.toString().includes(filter)));

        if (filteredResgates.length === 0) {
            container.innerHTML = `<p class="text-center text-gray-400 py-4">Nenhum resgate encontrado.</p>`;
            return;
        }
        
        // Ordena por data, o mais recente primeiro
        filteredResgates.sort((a, b) => new Date(b.data_geracao) - new Date(a.data_geracao));

        const rowsHtml = filteredResgates.map(p => {
            const hasComprovante = p.comprovante && p.comprovante.url;
            return `<tr>
                        <td>${formatApiDateToBR(p.data_geracao)}</td>
                        <td>${p.id_parceiro}</td>
                        <td>${p.parceiro}</td>
                        <td>${p.consultor || 'N/A'}</td>
                        <td class="text-right font-semibold">${formatCurrency(p.rt_valor)}<button class="edit-rt-btn text-blue-400 hover:text-blue-300 ml-2" title="Editar Valor RT" data-id="${p.id}"><span class="material-symbols-outlined text-base align-middle">edit</span></button></td>
                        <td class="text-center"><input type="checkbox" class="pagamento-status h-5 w-5 rounded bg-background-dark border-white/20 text-primary focus:ring-primary" data-id="${p.id}" ${p.pago ? 'checked' : ''}></td>
                        <td><div class="flex items-center gap-2"><label for="comprovante-input-${p.id}" class="file-input-label bg-white/10 hover:bg-white/20 text-xs py-1 px-3 !font-medium whitespace-nowrap">Anexar</label><input type="file" id="comprovante-input-${p.id}" class="comprovante-input file-input" data-id="${p.id}"><span class="file-status-text text-xs ${hasComprovante ? 'text-green-400 font-semibold' : 'text-gray-400'}">${hasComprovante ? 'Comprovante anexado' : 'Nenhum arquivo'}</span></div></td>
                        <td class="text-center"><button class="view-comprovante-btn text-primary/80 hover:text-primary font-semibold" data-id="${p.id}" ${!hasComprovante ? 'disabled' : ''} style="${!hasComprovante ? 'opacity: 0.5; cursor: not-allowed;' : ''}">Ver</button></td>
                    </tr>`;
        }).join('');
        
        container.innerHTML = `<div class="overflow-x-auto"><table><thead><tr><th>Data</th><th>ID Parceiro</th><th>Parceiro</th><th>Consultor</th><th class="text-right">Valor RT</th><th class="text-center">Pago</th><th>Anexar Comprovante</th><th class="text-center">Ver</th></tr></thead><tbody>${rowsHtml}</tbody></table></div>`;
    }
    
    renderRankingTable() {
        const container = document.getElementById('ranking-table-container');
        const ranking = this.arquitetos.map(a => ({ ...a, pontos: this.pontuacoes[a.id] || 0 })).sort((a, b) => b.pontos - a.pontos);
        if (ranking.length === 0) {
            container.innerHTML = `<p class="text-center text-gray-400">Nenhum arquiteto para exibir.</p>`; return;
        }
        const rows = ranking.map(a => `<tr><td>${a.id}</td><td>${a.nome}</td><td class="font-bold text-primary">${a.pontos}</td></tr>`).join('');
        container.innerHTML = `<table><thead><tr><th>ID</th><th>Nome</th><th>Pontos</th></tr></thead><tbody>${rows}</tbody></table>`;
    }
    
    populateArquitetoSelect() {
        const select = document.getElementById('arquiteto-select');
        select.innerHTML = '<option value="" class="bg-background-dark">Selecione um arquiteto</option>';
        this.arquitetos.sort((a, b) => a.nome.localeCompare(b.nome)).forEach(a => {
            select.innerHTML += `<option value="${a.id}" class="bg-background-dark">${a.nome}</option>`;
        });
    }

    renderArquivosImportados() {
        const container = document.getElementById('arquivos-importados-container');
        container.innerHTML = '';
        const dates = Object.keys(this.importedFiles).sort((a,b) => new Date(b.split('/').reverse().join('-')) - new Date(a.split('/').reverse().join('-')));
        if (dates.length === 0) {
            container.innerHTML = `<div class="glass-card rounded-lg p-6 text-center text-gray-400">Nenhum arquivo foi importado.</div>`; return;
        }
        dates.forEach(date => {
            const fileInfo = this.importedFiles[date];
            container.innerHTML += `<div class="imported-file-card"><div class="flex flex-wrap justify-between items-center gap-4"><div><h3 class="font-semibold text-lg text-white">Importação de ${date}</h3><p class="text-sm text-gray-400 mt-1">${fileInfo.name}</p></div><button class="download-arquivo-btn btn-modal !py-2 !px-4 !text-sm bg-indigo-500/80 hover:bg-indigo-500 flex items-center gap-2" data-date="${date}"><span class="material-symbols-outlined">download</span>Baixar</button></div></div>`;
        });
    }

    renderHistoricoManual() {
        const container = document.getElementById('historico-manual-container');
        if (!container) return;
    
        let rowsHtml = '';
        if (this.comissoesManuais.length === 0) {
            rowsHtml = `<tr><td colspan="7" class="text-center text-gray-400 py-4">Nenhuma comissão manual adicionada ainda.</td></tr>`;
        } else {
            rowsHtml = this.comissoesManuais.map(c => {
                const status = c.status || 'pendente';
                let statusColor, statusText;
                switch (status) {
                    case 'aprovada':
                        statusColor = 'bg-green-500/20 text-green-300'; statusText = 'Aprovada'; break;
                    case 'Recusada Gestão':
                        statusColor = 'bg-red-500/20 text-red-300'; statusText = 'Recusada'; break;
                    default:
                        statusColor = 'bg-yellow-500/20 text-yellow-300'; statusText = 'Pendente'; break;
                }
                return `
                <tr>
                    <td>${c.id_parceiro}</td>
                    <td><a href="#" class="view-comissao-details-btn text-primary/80 hover:text-primary font-semibold" data-comissao-id="${c.id}">${c.id_venda || 'N/A'}</a></td>
                    <td>${formatApiDateToBR(c.data_venda)}</td>
                    <td class="text-right">${formatCurrency(c.valor_venda)}</td>
                    <td title="${c.justificativa}">${(c.justificativa || '').substring(0, 30)}${c.justificativa && c.justificativa.length > 30 ? '...' : ''}</td>
                    <td>${c.consultor || ''}</td>
                    <td class="text-center"><span class="px-2 py-1 text-xs font-semibold rounded-full ${statusColor}">${statusText}</span></td>
                </tr>`;
            }).join('');
        }
    
        container.innerHTML = `
            <div class="max-h-[65vh] overflow-y-auto">
                <table>
                    <thead>
                        <tr>
                            <th>ID Parceiro</th>
                            <th>ID Venda</th>
                            <th>Data</th>
                            <th class="text-right">Valor</th>
                            <th>Justificativa</th>
                            <th>Consultor</th>
                            <th class="text-center">Status</th>
                        </tr>
                    </thead>
                    <tbody>${rowsHtml}</tbody>
                </table>
            </div>`;
    }
    
    renderResultados() {
        const todosPagamentos = Object.values(this.pagamentos).flat().concat(this.resgates);

        // Cálculos de RTs Pagas
        const pagamentosPagos = todosPagamentos.filter(p => p.pago);
        const totalRTsPagas = pagamentosPagos.reduce((sum, p) => sum + parseCurrency(p.rt_valor || 0), 0);
        const quantidadeRTsPagas = pagamentosPagos.length;
        const rtMedia = quantidadeRTsPagas > 0 ? totalRTsPagas / quantidadeRTsPagas : 0;

        // Cálculos de RTs a Pagar
        const pagamentosNaoPagos = todosPagamentos.filter(p => !p.pago);
        const valorEmPagamentosNaoPagos = pagamentosNaoPagos.reduce((sum, p) => sum + parseCurrency(p.rt_valor || 0), 0);
        const valorAcumuladoNaoGerado = this.arquitetos.reduce((sum, arq) => sum + (parseFloat(arq.rt_acumulado) || 0), 0);
        const totalRtAPagar = valorEmPagamentosNaoPagos + valorAcumuladoNaoGerado;
        const quantidadeRTsNaoPagas = pagamentosNaoPagos.length;


        // Atualização do DOM
        document.getElementById('total-rt').textContent = formatCurrency(totalRTsPagas);
        document.getElementById('total-rt-quantidade').textContent = quantidadeRTsPagas;
        document.getElementById('rt-media').textContent = formatCurrency(rtMedia);
        document.getElementById('total-rt-a-pagar').textContent = formatCurrency(totalRtAPagar);
        document.getElementById('total-rt-nao-pagas').textContent = quantidadeRTsNaoPagas;
    }
    
    renderSysledTable() {
        const container = document.getElementById('sysled-table-container');
        if (!container) return;
        if (this.sysledData.length === 0) {
             container.innerHTML = `<div class="text-center text-gray-400 py-8"><p>Clique em "Atualizar" para carregar as informações da API Sysled.</p></div>`; return;
        }
        const dataInicio = document.getElementById('sysled-filter-data-inicio').value;
        const dataFim = document.getElementById('sysled-filter-data-fim').value;
        const parceiro = document.getElementById('sysled-filter-parceiro').value.toLowerCase();
        const excluirParceiroInput = document.getElementById('sysled-filter-excluir-parceiro').value;
        const columnFilters = {};
        container.querySelectorAll('.sysled-column-filter').forEach(input => {
            if (input.value) columnFilters[input.dataset.column] = input.value.toLowerCase();
        });
        let dataToRender = [...this.sysledData];
        if (excluirParceiroInput) {
            const excludedIds = excluirParceiroInput.split(/[\s,]+/).map(id => id.trim()).filter(id => id);
            if (excludedIds.length > 0) {
                const partnerIdField = Object.keys(dataToRender[0]).find(k => k.toLowerCase().includes('parceirocodigo'));
                if (partnerIdField) dataToRender = dataToRender.filter(row => !excludedIds.includes(String(row[partnerIdField])));
            }
        }
        const dateField = Object.keys(dataToRender[0]).find(k => k.toLowerCase().includes('data'));
        if (dataInicio && dateField) dataToRender = dataToRender.filter(row => row[dateField] && row[dateField].split('T')[0] >= dataInicio);
        if (dataFim && dateField) dataToRender = dataToRender.filter(row => row[dateField] && row[dateField].split('T')[0] <= dataFim);
        if (parceiro) {
            const partnerNameField = Object.keys(dataToRender[0]).find(k => k.toLowerCase().includes('parceiro') || k.toLowerCase().includes('arquiteto'));
            const partnerIdField = Object.keys(dataToRender[0]).find(k => k.toLowerCase().includes('parceirocodigo'));
            dataToRender = dataToRender.filter(row => 
                (partnerNameField && row[partnerNameField] && row[partnerNameField].toLowerCase().includes(parceiro)) ||
                (partnerIdField && row[partnerIdField] && String(row[partnerIdField]).toLowerCase().includes(parceiro))
            );
        }
        Object.keys(columnFilters).forEach(column => {
            const filterValue = columnFilters[column];
            dataToRender = dataToRender.filter(row => row[column] != null && String(row[column]).toLowerCase().includes(filterValue));
        });
        this.sysledFilteredData = dataToRender;
        const headers = Object.keys(this.sysledData[0]);
        const headerHtml = headers.map(h => `<th>${h.replace(/_/g, ' ')}</th>`).join('');
        const filterHtml = headers.map(h => `<th><input type="text" class="sysled-column-filter w-full p-1 border rounded-md text-sm bg-background-dark/50 border-white/10" placeholder="Filtrar..." data-column="${h}" value="${columnFilters[h] ? columnFilters[h].replace(/"/g, '&quot;') : ''}"></th>`).join('');
        const rowsHtml = this.sysledFilteredData.length === 0
            ? `<tr><td colspan="${headers.length}" class="text-center text-gray-400 py-8">Nenhum resultado encontrado.</td></tr>`
            : this.sysledFilteredData.map(row => `<tr>${headers.map(h => {
                let cellValue = row[h];
                const lowerCaseHeader = h.toLowerCase();
                if (lowerCaseHeader.includes('data')) cellValue = formatApiDateToBR(cellValue);
                else if (lowerCaseHeader.includes('valor') || lowerCaseHeader.includes('total') || lowerCaseHeader.includes('valornota')) {
                    if (cellValue !== null && !isNaN(Number(String(cellValue)))) cellValue = formatApiNumberToBR(cellValue);
                }
                return `<td>${cellValue ?? ''}</td>`;
            }).join('')}</tr>`).join('');
        container.innerHTML = `<div class="max-h-[65vh] overflow-auto"><table><thead class="sticky top-0 z-10"><tr>${headerHtml}</tr><tr>${filterHtml}</tr></thead><tbody>${rowsHtml}</tbody></table></div>`;
    }

    renderSalesHistoryModal(salesData, isApiData) {
        const container = document.getElementById('sales-history-table-container');
        if (!salesData || salesData.length === 0) {
            container.innerHTML = `<p class="text-center text-gray-400 py-8">Nenhuma venda encontrada para este parceiro.</p>`; return;
        }
        const rowsHtml = salesData.map(sale => {
            const idCellContent = isApiData 
                ? `<a href="#" class="view-sale-details-btn text-primary/80 hover:text-primary" data-pedido-id="${sale.id_pedido}">${sale.id_pedido}</a>`
                : sale.id_pedido;
            return `<tr><td>${idCellContent}</td><td class="text-right">${formatCurrency(sale.valor_nota)}</td><td class="text-center">${formatApiDateToBR(sale.data_finalizacao_prevenda)}</td></tr>`
        }).join('');
        container.innerHTML = `<table><thead><tr><th>ID Pedido</th><th class="text-right">Valor da Nota</th><th class="text-center">Data da Venda</th></tr></thead><tbody>${rowsHtml}</tbody></table>`;
    }
    
    // NOVO: Renderiza a tabela de logs de eventos
    renderEventosLog() {
        const container = document.getElementById('eventos-log-container');
        if (!container) return;

        if (this.actionLogs.length === 0) {
            container.innerHTML = `<p class="text-center text-gray-400 py-8">Nenhum evento registrado.</p>`;
            return;
        }

        const rowsHtml = this.actionLogs.map(log => {
            const timestamp = new Date(log.when_did).toLocaleString('pt-BR');
            return `
                <tr>
                    <td>${log.who_did}</td>
                    <td>${log.what_did}</td>
                    <td>${timestamp}</td>
                </tr>
            `;
        }).join('');

        container.innerHTML = `
            <table>
                <thead>
                    <tr>
                        <th>Usuário</th>
                        <th>Ação</th>
                        <th>Quando</th>
                    </tr>
                </thead>
                <tbody>${rowsHtml}</tbody>
            </table>
        `;
    }

    /**
     * NOVO: Renderiza a aba de gerenciamento de permissões.
     */
    renderPermissionsTab() {
        const container = document.getElementById('permissions-container');
        if (!container) return;

        if (this.users.length === 0) {
            container.innerHTML = `<p class="text-center text-gray-400 py-8">Nenhum usuário encontrado.</p>`;
            return;
        }

        const rowsHtml = this.users.map(user => {
            // Define as roles disponíveis. Você pode ajustar conforme necessário.
            const roles = ['admin', 'gestor', 'user'];
            const optionsHtml = roles.map(role => 
                `<option value="${role}" ${user.role === role ? 'selected' : ''}>${role.charAt(0).toUpperCase() + role.slice(1)}</option>`
            ).join('');

            return `
                <tr>
                    <td>${user.email}</td>
                    <td>
                        <select class="glass-input w-full p-2 rounded-lg bg-background-dark/50" data-user-id="${user.id}">
                            ${optionsHtml}
                        </select>
                    </td>
                    <td class="text-center">
                        <button class="save-permission-btn btn-modal !py-1 !px-3 !text-xs bg-blue-500/80 hover:bg-blue-500" data-user-id="${user.id}">Salvar</button>
                    </td>
                </tr>`;
        }).join('');

        container.innerHTML = `<table><thead><tr><th>Email</th><th>Permissão</th><th class="text-center">Ações</th></tr></thead><tbody>${rowsHtml}</tbody></table>`;
    }

    checkPaymentFeature() {
        const btn = document.getElementById('gerar-pagamentos-rt-btn');
        if (!btn) return;
        const isEnabled = this.schemaHasRtAcumulado && this.schemaHasRtTotalPago;
        btn.disabled = !isEnabled;
        btn.title = isEnabled ? "Gerar pagamentos para arquitetos elegíveis" : "Funcionalidade desabilitada. Crie as colunas 'rt_acumulado' e 'rt_total_pago' no banco de dados.";
        btn.classList.toggle('opacity-50', !isEnabled);
        btn.classList.toggle('cursor-not-allowed', !isEnabled);
    }
    
    // --- MÉTODOS DE MANIPULAÇÃO DE MODAIS ---
    
    openRtMappingModal(headers) {
        const form = document.getElementById('rt-mapping-form');
        const modal = document.getElementById('rt-mapping-modal');
        form.innerHTML = '';
        const fields = { id_prevenda: 'ID Prevenda', data_venda: 'Data Venda', nome_cliente: 'Nome Cliente', valor_venda: 'Valor Venda', executivo: 'Executivo', id_parceiro: 'ID Parceiro', parceiro: 'Parceiro', loja: 'Loja' };
        const autoMap = { id_prevenda: 'idPedido', data_venda: 'dataFinalizacaoPrevenda', nome_cliente: 'clienteFantasia', valor_venda: 'valorNota', executivo: 'consultor', id_parceiro: 'idParceiro', parceiro: 'parceiro', loja: 'idEmpresa' };
        for (const key in fields) {
            const options = headers.map(h => `<option value="${h}" class="bg-background-dark">${h}</option>`).join('');
            form.innerHTML += `<div class="grid grid-cols-2 gap-4 items-center"><label for="map-${key}" class="font-medium text-gray-300">${fields[key]}</label><select id="map-${key}" name="${key}" class="glass-input w-full p-2 rounded-lg"><option value="" class="bg-background-dark">Selecione...</option>${options}</select></div>`;
            if (this.isSysledImport) {
                const select = form.querySelector(`#map-${key}`);
                if (select && headers.includes(autoMap[key])) select.value = autoMap[key];
            }
        }
        modal.onclick = (e) => {
            if (e.target === modal) this.closeRtMappingModal();
        };
        modal.classList.add('active');
    }

    closeRtMappingModal() {
        document.getElementById('rt-mapping-modal').classList.remove('active');
        const fileInput = document.getElementById('rt-file-input');
        if (fileInput) fileInput.value = '';
        document.getElementById('rt-file-name').textContent = '';
    }

    openArquitetoMappingModal(headers) {
        const form = document.getElementById('arquiteto-mapping-form');
        const modal = document.getElementById('arquiteto-mapping-modal');
        form.innerHTML = '';
        const fields = { id: 'ID', nome: 'Nome', email: 'Email', telefone: 'Telefone', chave_pix: 'Chave PIX', tipo_chave_pix: 'Tipo Chave PIX' };
        for (const key in fields) {
            const options = headers.map(h => `<option value="${h}" class="bg-background-dark">${h}</option>`).join('');
            form.innerHTML += `<div class="grid grid-cols-2 gap-4 items-center"><label class="font-medium text-gray-300">${fields[key]}</label><select name="${key}" class="glass-input w-full p-2 rounded-lg"><option value="" class="bg-background-dark">Selecione...</option>${options}</select></div>`;
        }
        modal.onclick = (e) => {
            if (e.target === modal) this.closeArquitetoMappingModal();
        };
        modal.classList.add('active');
    }

    closeArquitetoMappingModal() {
        document.getElementById('arquiteto-mapping-modal').classList.remove('active');
        const fileInput = document.getElementById('arquiteto-file-input');
        if (fileInput) fileInput.value = '';
        document.getElementById('file-name-arquitetos').textContent = '';
    }

    openEditModal(id) {
        const arquiteto = this.arquitetos.find(a => String(a.id) === String(id));
        if (!arquiteto) return;
        document.getElementById('edit-arquiteto-original-id').value = arquiteto.id;
        document.getElementById('edit-arquiteto-id').textContent = `ID: ${arquiteto.id}`;
        document.getElementById('edit-arquiteto-nome').value = arquiteto.nome || '';
        document.getElementById('edit-arquiteto-email').value = arquiteto.email || '';
        document.getElementById('edit-arquiteto-telefone').value = arquiteto.telefone || '';
        document.getElementById('edit-arquiteto-pix').value = arquiteto.pix || '';
        document.getElementById('edit-arquiteto-tipo-pix').value = arquiteto.tipo_chave_pix || '';
        document.getElementById('edit-arquiteto-vendas').value = arquiteto.salesCount || 0;
        document.getElementById('rt-valor-vendas').textContent = formatCurrency(arquiteto.valorVendasTotal || 0);
        document.getElementById('rt-percentual').value = arquiteto.rtPercentual || 0.05;
        if(this.schemaHasRtAcumulado) document.getElementById('edit-arquiteto-rt-acumulado').textContent = formatCurrency(arquiteto.rt_acumulado || 0);
        if(this.schemaHasRtTotalPago) document.getElementById('edit-arquiteto-rt-total-pago').textContent = formatCurrency(arquiteto.rt_total_pago || 0);
        const modal = document.getElementById('edit-arquiteto-modal');
        modal.onclick = (e) => {
            if (e.target === modal) this.closeEditModal();
        };
        modal.classList.add('active');
        this.calculateRT();
    }

    closeEditModal() {
        document.getElementById('edit-arquiteto-modal').classList.remove('active');
    }
    
    openAddValueModal(id) {
        const arquiteto = this.arquitetos.find(a => a.id === id);
        if (!arquiteto) return;
        document.getElementById('add-value-modal-title').textContent = `Adicionar Venda Manual para ${arquiteto.nome}`;
        document.getElementById('add-value-arquiteto-id').value = id;
        const modal = document.getElementById('add-value-modal');
        modal.onclick = (e) => {
            if (e.target === modal) this.closeAddValueModal();
        };
        modal.classList.add('active');
    }

    closeAddValueModal() {
        const modal = document.getElementById('add-value-modal');
        modal.classList.remove('active');
        document.getElementById('add-value-form').reset();
    }
    
    openComprovanteModal(pagamentoId, type = 'pagamento') {
        let pagamento;
        if (type === 'resgate') {
            pagamento = this.resgates.find(p => p.id.toString() === pagamentoId);
        } else {
            pagamento = Object.values(this.pagamentos).flat().find(p => p.id.toString() === pagamentoId);
        }

        if (!pagamento) return;

        document.getElementById('comprovante-modal-title').textContent = `Detalhes de Pagamento para ${pagamento.parceiro}`;
        document.getElementById('comprovante-valor-rt').textContent = formatCurrency(pagamento.rt_valor || 0);
        const imgContainer = document.getElementById('comprovante-img-container');
        imgContainer.innerHTML = (pagamento.comprovante && pagamento.comprovante.url)
            ? `<img src="${pagamento.comprovante.url}" alt="${pagamento.comprovante.name}" class="max-w-full max-h-96 object-contain rounded-lg">`
            : `<p class="text-gray-400">Nenhum comprovante anexado.</p>`;
        const modal = document.getElementById('comprovante-modal');
        modal.onclick = (e) => {
            if (e.target === modal) this.closeComprovanteModal();
        };
        modal.classList.add('active');
    }
    
    closeComprovanteModal() {
        document.getElementById('comprovante-modal').classList.remove('active');
    }

    openGerarPagamentosModal() {
        const container = document.getElementById('gerar-pagamentos-table-container');
        const rowsHtml = this.eligibleForPayment.map(a => `
            <tr><td>${a.id}</td><td>${a.nome}</td><td class="text-right font-semibold text-primary">${formatCurrency(a.rt_acumulado || 0)}</td><td>${(a.tipo_chave_pix ? a.tipo_chave_pix + ': ' : '') + (a.pix || 'Não cadastrado')}</td></tr>`).join('');
        container.innerHTML = `<table><thead><tr><th>ID</th><th>Nome</th><th class="text-right">Valor a Pagar</th><th>Chave PIX</th></tr></thead><tbody>${rowsHtml}</tbody></table>`;
        const modal = document.getElementById('gerar-pagamentos-modal');
        modal.onclick = (e) => {
            if (e.target === modal) this.closeGerarPagamentosModal();
        };
        modal.classList.add('active');
    }

    closeGerarPagamentosModal() {
        document.getElementById('gerar-pagamentos-modal').classList.remove('active');
    }

    openSaleDetailsModal(pedidoId) {
        if (!pedidoId || pedidoId === 'N/A') { alert("ID do Pedido inválido."); return; }
        const saleData = this.sysledData.find(row => String(row.idPedido) === String(pedidoId));
        if (!saleData) { alert(`Detalhes para o pedido ${pedidoId} não foram encontrados.`); return; }
        document.getElementById('sale-details-modal-title').textContent = `Detalhes da Venda - Pedido ${pedidoId}`;
        document.getElementById('import-single-sale-btn').dataset.pedidoId = pedidoId;
        const detailsHtml = Object.entries(saleData).map(([key, value]) => `<tr><td class="p-2 font-semibold text-gray-300 align-top">${key}</td><td class="p-2 text-gray-100">${value ?? ''}</td></tr>`).join('');
        document.getElementById('sale-details-content').innerHTML = `<table class="w-full text-sm"><tbody>${detailsHtml}</tbody></table>`;
        const modal = document.getElementById('sale-details-modal');
        modal.onclick = (e) => {
            if (e.target === modal) this.closeSaleDetailsModal();
        };
        modal.classList.add('active');
    }

    closeSaleDetailsModal() {
        document.getElementById('sale-details-modal').classList.remove('active');
    }
    closeSalesHistoryModal() {
        document.getElementById('sales-history-modal').classList.remove('active');
    }

    openComissaoManualDetailsModal(comissaoId) {
        const comissao = this.comissoesManuais.find(c => c.id === comissaoId);
        if (!comissao) { alert('Detalhes da comissão não encontrados.'); return; }
        const arquiteto = this.arquitetos.find(a => a.id === comissao.id_parceiro);
        const status = comissao.status || 'pendente';

        let statusColor;
        if (status === 'aprovada') {
            statusColor = 'bg-green-500/20 text-green-300';
        } else if (status === 'Recusada Gestão') {
            statusColor = 'bg-red-500/20 text-red-300';
        } else {
            statusColor = 'bg-yellow-500/20 text-yellow-300';
        }

        const content = [
            { label: 'ID Parceiro', value: comissao.id_parceiro },
            { label: 'Nome Parceiro', value: arquiteto ? arquiteto.nome : 'Não encontrado' },
            { label: 'ID Venda', value: comissao.id_venda || 'N/A' },
            { label: 'Valor Venda', value: formatCurrency(comissao.valor_venda) },
            { label: 'Data Venda', value: formatApiDateToBR(comissao.data_venda) },
            { label: 'Consultor', value: comissao.consultor || 'N/A' },
            { label: 'Justificativa', value: comissao.justificativa, pre: true },
            { label: 'Status', value: `<span class="px-2 py-1 text-xs font-semibold rounded-full ${statusColor}">${status}</span>` }
        ].map(item => `<div class="grid grid-cols-3 gap-2"><p class="font-medium text-gray-400 col-span-1">${item.label}:</p><div class="col-span-2 ${item.pre ? 'whitespace-pre-wrap' : ''}">${item.value}</div></div>`).join('');
        document.getElementById('comissao-manual-details-content').innerHTML = content;
        
        const approveBtn = document.getElementById('aprovar-inclusao-manual-btn');
        approveBtn.dataset.comissaoId = comissaoId;
        approveBtn.style.display = status === 'aprovada' ? 'none' : 'inline-block';

        const modal = document.getElementById('comissao-manual-details-modal');
        modal.onclick = (e) => {
            if (e.target === modal) this.closeComissaoManualDetailsModal();
        };
        modal.classList.add('active');
    }

    closeComissaoManualDetailsModal() {
        document.getElementById('comissao-manual-details-modal').classList.remove('active');
    }

    openEditRtModal(pagamentoId, type = 'pagamento') {
        let pagamento;
        if (type === 'resgate') {
            pagamento = this.resgates.find(p => p.id.toString() === pagamentoId);
        } else {
            pagamento = Object.values(this.pagamentos).flat().find(p => p.id.toString() === pagamentoId);
        }
        if (!pagamento) return;

        const form = document.getElementById('edit-rt-form');
        form.dataset.type = type; // Armazena o tipo no dataset do formulário
        document.getElementById('edit-rt-pagamento-id').value = pagamento.id;
        document.getElementById('edit-rt-input').value = parseCurrency(pagamento.rt_valor);
        const modal = document.getElementById('edit-rt-modal');
        modal.onclick = (e) => {
            if (e.target === modal) this.closeEditRtModal();
        };
        modal.classList.add('active');
    }

    closeEditRtModal() {
        const modal = document.getElementById('edit-rt-modal');
        modal.classList.remove('active');
        document.getElementById('edit-rt-form').reset();
    }
    
    // --- MÉTODOS DE LÓGICA DE NEGÓCIO E MANIPULAÇÃO DE DADOS ---

    // NOVO: Função para registrar uma ação no log de eventos
    async logAction(actionDescription) {
        const { error } = await supabase.from('action_logs').insert({
            who_did: this.currentUserEmail,
            what_did: actionDescription
        });
        if (error) {
            console.error('Erro ao registrar ação no log:', error);
        }
    }
    
    handleArquitetosTableClick(e) {
        const idLink = e.target.closest('.id-link');
        const editBtn = e.target.closest('.edit-btn');
        const deleteBtn = e.target.closest('.delete-btn');
        const addValueBtn = e.target.closest('.add-value-btn');
        if (idLink) { e.preventDefault(); this.openEditModal(idLink.dataset.id); }
        if (editBtn) this.openEditModal(editBtn.dataset.id);
        if (deleteBtn) this.deleteArquiteto(deleteBtn.dataset.id);
        if (addValueBtn) this.openAddValueModal(addValueBtn.dataset.id);
    }
    
    handleRTFileSelect(event) {
        this.isSysledImport = false;
        const file = event.target.files[0];
        if (!file) return;
        document.getElementById('rt-file-name').textContent = `Arquivo: ${file.name}`;
        const reader = new FileReader();
        reader.onload = (e) => {
            const data = new Uint8Array(e.target.result);
            const workbook = XLSX.read(data, { type: 'array' });
            this.tempRTData = XLSX.utils.sheet_to_json(workbook.Sheets[workbook.SheetNames[0]], { raw: false });
            const headers = this.tempRTData.length > 0 ? Object.keys(this.tempRTData[0]) : [];
            this.openRtMappingModal(headers);
        };
        reader.readAsArrayBuffer(file);
    }

    async handleRtMapping() {
        const mapping = {};
        document.getElementById('rt-mapping-form').querySelectorAll('select').forEach(s => { mapping[s.name] = s.value; });
        if (!mapping.id_parceiro || !mapping.valor_venda) {
            alert("Os campos 'ID Parceiro' e 'Valor Venda' são obrigatórios."); return;
        }
        if (this.isSysledImport && !mapping.id_prevenda) {
            alert("O 'ID Prevenda' é obrigatório para importações Sysled para evitar duplicatas."); return;
        }
        let processedData = this.tempRTData.map(row => {
            const newRow = {};
            for (const key in mapping) { if (mapping[key]) newRow[key] = row[mapping[key]]; }
            if (this.isSysledImport) newRow.valor_venda = parseApiNumber(newRow.valor_venda);
            return newRow;
        });
        
        if (this.isSysledImport) {
            const pedidoIds = processedData.map(row => row.id_prevenda).filter(id => id);
            if (pedidoIds.length > 0) {
                const { data: existing, error } = await supabase.from('sysled_imports').select('id_pedido').in('id_pedido', pedidoIds);
                if (error) { alert('Erro ao verificar vendas existentes: ' + error.message); this.closeRtMappingModal(); return; }
                const existingIds = new Set(existing.map(item => String(item.id_pedido)));
                const alreadyImported = processedData.filter(row => existingIds.has(String(row.id_prevenda)));
                processedData = processedData.filter(row => !existingIds.has(String(row.id_prevenda)));
                if (alreadyImported.length > 0) alert(`Venda(s) já importada(s) e ignorada(s): ${alreadyImported.map(r => r.id_prevenda).join(', ')}`);
            }
        }
        if (processedData.length > 0) {
            await this.processRTData(processedData);
        } else {
            alert("Nenhuma venda nova para importar.");
        }
        this.closeRtMappingModal();
    }

    async processRTData(data) {
        const todayKey = new Date().toLocaleDateString('pt-BR');
        const todayDB = new Date().toISOString().slice(0, 10);
        let fileToSave = null;

        if (this.isSysledImport) {
            fileToSave = { name: `importacao_sysled_${todayKey.replace(/\//g, '-')}.xlsx`, dataUrl: jsonToXLSXDataURL(this.tempRTData) };
        } else {
            const file = document.getElementById('rt-file-input').files[0];
            if (file) fileToSave = { name: file.name, dataUrl: await fileToBase64(file) };
        }

        if (fileToSave) {
            const { data: fileData, error } = await supabase.from('arquivos_importados').insert({ data_importacao: todayDB, name: fileToSave.name, dataUrl: fileToSave.dataUrl }).select().single();
            if (error) console.error("Erro ao salvar arquivo:", error);
            else {
                this.importedFiles[todayKey] = { name: fileData.name, dataUrl: fileData.dataUrl, id: fileData.id };
                await this.logAction(`Importou o arquivo: ${fileToSave.name}`);
            }
        }

        const architectUpdates = {};
        for (const record of data) {
            const partnerId = String(record.id_parceiro);
            if (!partnerId) continue;
            const valorVenda = parseCurrency(record.valor_venda);
            let arquiteto = this.arquitetos.find(a => a.id === partnerId);
            if (!arquiteto) {
                const newArquitetoData = { id: partnerId, nome: record.parceiro || 'Novo Parceiro', salesCount: 0, valorVendasTotal: 0, pontos: 0, rtPercentual: 0.05, rt_acumulado: 0, rt_total_pago: 0 };
                const { data: created, error } = await supabase.from('arquitetos').insert(newArquitetoData).select().single();
                if (error) { console.error(`Erro ao criar arquiteto ${partnerId}:`, error); continue; }
                this.arquitetos.push(created);
                arquiteto = created;
                await this.logAction(`Criou novo arquiteto (ID: ${partnerId}) via importação.`);
            }
            if (!architectUpdates[partnerId]) architectUpdates[partnerId] = { valorVendasTotal: arquiteto.valorVendasTotal || 0, salesCount: arquiteto.salesCount || 0, pontos: arquiteto.pontos || 0, rt_acumulado: parseFloat(arquiteto.rt_acumulado || 0) };
            architectUpdates[partnerId].valorVendasTotal += valorVenda;
            architectUpdates[partnerId].salesCount += 1;
            architectUpdates[partnerId].pontos += Math.floor(valorVenda / 1000);
            if (this.schemaHasRtAcumulado) architectUpdates[partnerId].rt_acumulado += valorVenda * (arquiteto.rtPercentual || 0.05);
        }
        await Promise.all(Object.keys(architectUpdates).map(id => supabase.from('arquitetos').update(architectUpdates[id]).eq('id', id)));
        
        if (this.isSysledImport) {
            const payload = data.map(row => ({
                id_parceiro: row.id_parceiro,
                valor_nota: row.valor_venda,
                data_finalizacao_prevenda: row.data_venda,
                id_pedido: row.id_prevenda,
                consultor: row.executivo // Adicionado o campo consultor
            }));
            const { error } = await supabase.from('sysled_imports').insert(payload);
            if (error) {
                alert("AVISO: Os dados dos arquitetos foram atualizados, mas ocorreu um erro ao salvar o histórico de importação para evitar duplicatas. Vendas podem ser importadas novamente no futuro. Erro: " + error.message);
                console.error("Erro ao salvar na tabela sysled_imports:", error);
            }
        }
        
        alert('Dados de vendas processados com sucesso!');
        await this.loadData();
        this.renderAll();
        this.isSysledImport = false;
    }
    
    async handleAddArquiteto(e) {
        e.preventDefault();
        const id = document.getElementById('arquiteto-id').value;
        const nome = document.getElementById('arquiteto-nome').value;
        if (this.arquitetos.some(a => a.id === id)) { alert('ID já existe.'); return; }
        const newArquiteto = { id, nome, email: document.getElementById('arquiteto-email').value, telefone: document.getElementById('arquiteto-telefone').value, pix: document.getElementById('arquiteto-pix').value, salesCount: 0, valorVendasTotal: 0, pontos: 0, rtPercentual: 0.05, rt_acumulado: 0, rt_total_pago: 0 };
        const { data, error } = await supabase.from('arquitetos').insert(newArquiteto).select().single();
        if (error) { alert('Erro: ' + error.message); }
        else {
            this.arquitetos.push(data);
            this.pontuacoes[data.id] = data.pontos;
            await this.logAction(`Adicionou o arquiteto: ${nome} (ID: ${id})`);
            this.renderAll();
            e.target.reset();
        }
    }

    handleArquitetoFileUpload(event) {
        const file = event.target.files[0];
        if (!file) return;
        document.getElementById('file-name-arquitetos').textContent = `Arquivo: ${file.name}`;
        const reader = new FileReader();
        reader.onload = (e) => {
            const data = new Uint8Array(e.target.result);
            const workbook = XLSX.read(data, { type: 'array' });
            this.tempArquitetoData = XLSX.utils.sheet_to_json(workbook.Sheets[workbook.SheetNames[0]], { raw: false });
            const headers = this.tempArquitetoData.length > 0 ? Object.keys(this.tempArquitetoData[0]) : [];
            this.openArquitetoMappingModal(headers);
        };
        reader.readAsArrayBuffer(file);
    }
    
    async handleArquitetoMapping() {
        const mapping = {};
        document.getElementById('arquiteto-mapping-form').querySelectorAll('select').forEach(s => { mapping[s.name] = s.value; });

        if (!mapping.id || !mapping.nome) {
            alert("Os campos 'ID' e 'Nome' são obrigatórios no mapeamento.");
            return;
        }

        const novosArquitetos = [];
        const arquitetosParaAtualizar = [];

        this.tempArquitetoData.forEach(row => {
            const id = String(row[mapping.id] || '');
            if (!id) return; // Pula linhas sem ID

            const arquitetoData = {
                id: id,
                nome: row[mapping.nome],
                email: row[mapping.email] || null,
                telefone: row[mapping.telefone] || null,
                pix: row[mapping.chave_pix] || null,
                tipo_chave_pix: row[mapping.tipo_chave_pix] || null
            };

            const arquitetoExistente = this.arquitetos.find(a => a.id === id);

            if (arquitetoExistente) {
                arquitetosParaAtualizar.push(arquitetoData);
            } else {
                novosArquitetos.push({
                    ...arquitetoData,
                    salesCount: 0,
                    valorVendasTotal: 0,
                    pontos: 0,
                    rtPercentual: 0.05,
                    rt_acumulado: 0,
                    rt_total_pago: 0
                });
            }
        });

        let success = true;
        let errorMessage = '';
        let novosCount = 0;
        let atualizadosCount = 0;

        try {
            if (novosArquitetos.length > 0) {
                const { error } = await supabase.from('arquitetos').insert(novosArquitetos);
                if (error) throw error;
                novosCount = novosArquitetos.length;
            }

            if (arquitetosParaAtualizar.length > 0) {
                const updatePromises = arquitetosParaAtualizar.map(arq =>
                    supabase.from('arquitetos').update({
                        nome: arq.nome,
                        email: arq.email,
                        telefone: arq.telefone,
                        pix: arq.pix,
                        tipo_chave_pix: arq.tipo_chave_pix
                    }).eq('id', arq.id)
                );
                const results = await Promise.all(updatePromises);
                const updateErrors = results.filter(res => res.error);
                if (updateErrors.length > 0) {
                    throw new Error(updateErrors.map(e => e.error.message).join(', '));
                }
                atualizadosCount = arquitetosParaAtualizar.length;
            }

        } catch (error) {
            success = false;
            errorMessage = error.message;
        }

        if (success) {
            let alertMessage = '';
            if (novosCount > 0) alertMessage += `${novosCount} novos arquitetos importados.\n`;
            if (atualizadosCount > 0) alertMessage += `${atualizadosCount} arquitetos atualizados.\n`;
            if (novosCount === 0 && atualizadosCount === 0) alertMessage = 'Nenhum arquiteto para importar ou atualizar.';
            
            alert(alertMessage.trim());
            await this.logAction(`Importou ${novosCount} e atualizou ${atualizadosCount} arquitetos via planilha.`);
            await this.loadData();
            this.renderAll();
        } else {
            alert("Ocorreu um erro durante o processo:\n" + errorMessage);
        }

        this.closeArquitetoMappingModal();
    }

    async handleEditArquiteto(e) {
        e.preventDefault();
        const originalId = document.getElementById('edit-arquiteto-original-id').value;
        const arquiteto = this.arquitetos.find(a => a.id === originalId);
        if (!arquiteto) return;
        
        const tipoPixValue = document.getElementById('edit-arquiteto-tipo-pix').value;
        const updatedData = {
            nome: document.getElementById('edit-arquiteto-nome').value,
            email: document.getElementById('edit-arquiteto-email').value,
            telefone: document.getElementById('edit-arquiteto-telefone').value,
            pix: document.getElementById('edit-arquiteto-pix').value,
            tipo_chave_pix: tipoPixValue || null,
            salesCount: parseInt(document.getElementById('edit-arquiteto-vendas').value, 10) || 0,
            rtPercentual: parseFloat(document.getElementById('rt-percentual').value)
        };

        if (this.schemaHasRtAcumulado && updatedData.rtPercentual !== arquiteto.rtPercentual) {
            updatedData.rt_acumulado = (arquiteto.valorVendasTotal || 0) * updatedData.rtPercentual - (arquiteto.rt_total_pago || 0);
        }
        const { data, error } = await supabase.from('arquitetos').update(updatedData).eq('id', originalId).select().single();
        if (error) { alert("Erro ao salvar: " + error.message); }
        else {
            const index = this.arquitetos.findIndex(a => a.id === originalId);
            this.arquitetos[index] = { ...this.arquitetos[index], ...data };
            await this.logAction(`Editou o arquiteto: ${updatedData.nome} (ID: ${originalId})`);
            this.renderAll();
            this.closeEditModal();
        }
    }

    async deleteArquiteto(id) {
        const arq = this.arquitetos.find(a => a.id === id);
        if (!arq) return;
        if (confirm(`Tem certeza que deseja apagar o arquiteto ${arq.nome} (ID: ${id})?`)) {
            const { error } = await supabase.from('arquitetos').delete().eq('id', id);
            if (error) { alert("Erro ao apagar: " + error.message); }
            else {
                this.arquitetos = this.arquitetos.filter(a => a.id !== id);
                delete this.pontuacoes[id];
                await this.logAction(`Apagou o arquiteto: ${arq.nome} (ID: ${id})`);
                this.renderAll();
            }
        }
    }

    async deleteAllArquitetos() {
        if (confirm('TEM CERTEZA? Esta ação apagará TODOS os dados de forma irreversível.')) {
            const [arq, pag, file, comiss] = await Promise.all([
                supabase.from('arquitetos').delete().neq('id', '0'),
                supabase.from('pagamentos').delete().neq('id', 0),
                supabase.from('arquivos_importados').delete().neq('id', 0),
                supabase.from('comissoes_manuais').delete().neq('id', 0)
            ]);
            const errors = [arq.error, pag.error, file.error, comiss.error].filter(Boolean);
            if (errors.length > 0) alert("Ocorreram erros: " + errors.map(e => e.message).join('\n'));
            else {
                alert('Todos os dados foram apagados com sucesso.');
                await this.logAction(`APAGOU TODOS OS DADOS DO SISTEMA.`);
            }
            await this.loadData();
            this.renderAll();
        }
    }

    exportArquitetosCSV() {
        if (this.arquitetos.length === 0) { alert("Não há dados para exportar."); return; }
        const data = this.arquitetos.map(a => {
            const row = { id: a.id, nome: a.nome, email: a.email, telefone: a.telefone, tipo_chave_pix: a.tipo_chave_pix, pix: a.pix, quantidade_vendas: a.salesCount || 0, valor_total_vendas: a.valorVendasTotal || 0, pontos: this.pontuacoes[a.id] || 0 };
            if (this.schemaHasRtAcumulado) row.rt_acumulado = a.rt_acumulado || 0;
            if (this.schemaHasRtTotalPago) row.rt_total_pago = a.rt_total_pago || 0;
            return row;
        });
        const ws = XLSX.utils.json_to_sheet(data);
        const wb = XLSX.utils.book_new();
        XLSX.utils.book_append_sheet(wb, ws, "Arquitetos");
        XLSX.writeFile(wb, "cadastro_arquitetos.xlsx");
        this.logAction("Exportou a lista de arquitetos para CSV.");
    }

    async handleAddValue(e) {
        e.preventDefault();
        const id = document.getElementById('add-value-arquiteto-id').value;
        const value = parseFloat(document.getElementById('add-value-input').value);
        const arq = this.arquitetos.find(a => a.id === id);
        if (arq && !isNaN(value)) {
            const payload = {
                valorVendasTotal: (arq.valorVendasTotal || 0) + value,
                pontos: (this.pontuacoes[id] || 0) + Math.floor(value / 1000),
                salesCount: (arq.salesCount || 0) + 1,
                ...(this.schemaHasRtAcumulado && { rt_acumulado: parseFloat(arq.rt_acumulado || 0) + (value * (arq.rtPercentual || 0.05)) })
            };
            const { data, error } = await supabase.from('arquitetos').update(payload).eq('id', id).select().single();
            if(error) { alert("Erro: " + error.message); }
            else {
                const index = this.arquitetos.findIndex(a => a.id === id);
                this.arquitetos[index] = data;
                this.pontuacoes[id] = data.pontos;
                await this.logAction(`Adicionou venda manual de ${formatCurrency(value)} para ${arq.nome} (ID: ${id})`);
                this.renderAll();
                this.closeAddValueModal();
            }
        }
    }
    
    async handleAddPontos(e) {
        e.preventDefault();
        const id = document.getElementById('arquiteto-select').value;
        const pontos = parseInt(document.getElementById('pontos-valor').value, 10);
        const arq = this.arquitetos.find(a => a.id === id);
        if (arq && !isNaN(pontos)) {
            const newPoints = (this.pontuacoes[id] || 0) + pontos;
            const { error } = await supabase.from('arquitetos').update({ pontos: newPoints }).eq('id', id);
            if (error) { alert("Erro: " + error.message); }
            else {
                this.pontuacoes[id] = newPoints;
                arq.pontos = newPoints;
                await this.logAction(`Ajustou ${pontos} pontos para ${arq.nome} (ID: ${id})`);
                this.renderRankingTable();
                e.target.reset();
            }
        }
    }

    handlePagamentosChange(e) {
        const target = e.target;
        if (!target.matches('.pagamento-status, .comprovante-input')) return;
    
        const container = target.closest('#pagamentos-container, #resgates-container');
        if (!container) return;
        const type = container.id === 'resgates-container' ? 'resgate' : 'pagamento';
        const { id } = target.dataset;
    
        if (target.matches('.pagamento-status')) {
            this.updatePagamentoStatus(id, target.checked, type);
        }
        if (target.matches('.comprovante-input')) {
            const statusSpan = target.parentElement.querySelector('.file-status-text');
            if (target.files.length > 0 && statusSpan) {
                statusSpan.textContent = 'Comprovante anexado';
                statusSpan.className = 'file-status-text text-xs text-green-400 font-semibold';
            }
            this.handleComprovanteUpload(id, target.files[0], type);
        }
    }

    async handlePagamentosClick(e) { // <-- FUNÇÃO MODIFICADA
        const btn = e.target.closest('button');
        if (!btn) return;
    
        const container = btn.closest('#pagamentos-container, #resgates-container');
        if (!container) return;
        const type = container.id === 'resgates-container' ? 'resgate' : 'pagamento';
        const { date, id } = btn.dataset;
    
        if (btn.matches('.view-comprovante-btn') && !btn.disabled) { 
            e.preventDefault(); 
            this.openComprovanteModal(id, type); 
        }
        if (btn.matches('.edit-rt-btn')) { 
            this.openEditRtModal(id, type); 
        }
    
        // Ações específicas para a view de pagamentos (lotes)
        if (type === 'pagamento') {
            if (btn.matches('.delete-pagamentos-btn')) this.deletePagamentosGroup(date);
            if (btn.matches('.download-xlsx-btn')) this.exportPagamentosXLSX(date);
            if (btn.matches('.gerar-relatorio-btn')) {
                await this.generatePagamentoPrint(date); // <-- CHAMADA MODIFICADA
            }
        }
    }

    async updatePagamentoStatus(pagamentoId, isChecked, type) {
        let pagamento;
        if (type === 'resgate') {
            pagamento = this.resgates.find(p => p.id.toString() === pagamentoId);
        } else {
            pagamento = Object.values(this.pagamentos).flat().find(p => p.id.toString() === pagamentoId);
        }

        if (pagamento) {
            const { error } = await supabase.from('pagamentos').update({ pago: isChecked }).eq('id', pagamento.id);
            if (error) alert("Erro: " + error.message);
            else {
                pagamento.pago = isChecked;
                await this.logAction(`Marcou ${type} (ID: ${pagamentoId}) para ${pagamento.parceiro} como ${isChecked ? 'PAGO' : 'NÃO PAGO'}.`);
                this.renderResultados();
            }
        }
    }

    async handleComprovanteUpload(pagamentoId, file, type) {
        if (!file) return;

        let pagamento;
        if (type === 'resgate') {
            pagamento = this.resgates.find(p => p.id.toString() === pagamentoId);
        } else {
            pagamento = Object.values(this.pagamentos).flat().find(p => p.id.toString() === pagamentoId);
        }
        
        if(pagamento){
            const dataUrl = await fileToBase64(file);
            pagamento.comprovante = { name: file.name, url: dataUrl };
            const { error } = await supabase.from('pagamentos').update({ comprovante: pagamento.comprovante }).eq('id', pagamento.id);
            if(error) alert("Erro: " + error.message);
            else {
                await this.logAction(`Anexou comprovante para o ${type} (ID: ${pagamentoId}) de ${pagamento.parceiro}.`);
                type === 'resgate' ? this.renderResgates() : this.renderPagamentos();
            }
        }
    }
    
    async deletePagamentosGroup(date) {
        if (confirm(`Tem certeza que deseja apagar os pagamentos de ${date}?`)) {
            const ids = this.pagamentos[date].map(p => p.id);
            const { error } = await supabase.from('pagamentos').delete().in('id', ids);
            if (error) { alert("Erro: " + error.message); }
            else {
                delete this.pagamentos[date];
                await this.logAction(`Apagou o lote de pagamentos gerado em ${date}.`);
                this.renderPagamentos();
            }
        }
    }

    async handleUpdateRtValue(e) {
        e.preventDefault();
        const form = e.target;
        const id = document.getElementById('edit-rt-pagamento-id').value;
        const type = form.dataset.type;
        const newValue = parseFloat(document.getElementById('edit-rt-input').value);
        if (isNaN(newValue) || newValue < 0) { alert('Valor inválido.'); return; }
        
        let pagamento;
        if (type === 'resgate') {
            pagamento = this.resgates.find(p => p.id.toString() === id);
        } else {
            pagamento = Object.values(this.pagamentos).flat().find(p => p.id.toString() === id);
        }

        if (pagamento) {
            const oldValue = pagamento.rt_valor;
            const { error } = await supabase.from('pagamentos').update({ rt_valor: newValue }).eq('id', pagamento.id);
            if (error) { alert("Erro: " + error.message); }
            else {
                pagamento.rt_valor = newValue;
                await this.logAction(`Alterou valor do ${type} (ID: ${id}) de ${formatCurrency(oldValue)} para ${formatCurrency(newValue)}.`);
                type === 'resgate' ? this.renderResgates() : this.renderPagamentos();
                this.renderResultados();
                this.closeEditRtModal();
                alert('Valor atualizado!');
            }
        }
    }

    exportPagamentosXLSX(date) {
        const data = this.pagamentos[date];
        if (!data || data.length === 0) { alert("Sem dados para exportar."); return; }
        const reportData = data.map(p => ({
            'ID Parceiro': p.id_parceiro,
            'Parceiro': p.parceiro,
            'Consultor': p.consultor || '',
            'Valor RT': parseCurrency(p.rt_valor),
            'Pago': p.pago ? 'Sim' : 'Não',
            'Data Geração': p.data_geracao
        }));
        const ws = XLSX.utils.json_to_sheet(reportData);
        const wb = XLSX.utils.book_new();
        XLSX.utils.book_append_sheet(wb, ws, "Pagamentos");
        XLSX.writeFile(wb, `Pagamentos_${date.replace(/\//g, '-')}.xlsx`);
        this.logAction(`Exportou o relatório de pagamentos de ${date}.`);
    }

    async generatePagamentoPrint(date) { // <-- FUNÇÃO MODIFICADA
        const data = this.pagamentos[date];
        if (!data || data.length === 0) { alert('Sem dados para gerar relatório.'); return; }
        const total = data.reduce((sum, p) => sum + parseCurrency(p.rt_valor || 0), 0);

        // 1. Coletar todos os IDs de parceiros deste lote de pagamento
        const partnerIds = [...new Set(data.map(p => p.id_parceiro))];

        // 2. Buscar os pedidos mais recentes para todos esses parceiros de uma só vez
        let latestPedidoMap = {};
        if (partnerIds.length > 0) {
            const { data: pedidoData, error } = await supabase
                .from('sysled_imports')
                .select('id_parceiro, id_pedido, created_at')
                .in('id_parceiro', partnerIds)
                .order('created_at', { ascending: false }); // Ordena pelo mais recente

            if (error) {
                console.error("Erro ao buscar IDs de pedido:", error);
                alert("Erro ao buscar IDs de pedido. O relatório será gerado sem eles.");
            } else {
                // 3. Processar os resultados para agrupar pedidos com o mesmo timestamp
                pedidoData.forEach(rec => {
                    const partnerId = rec.id_parceiro;
                    if (!latestPedidoMap[partnerId]) {
                        // Se é o primeiro (e mais recente) que encontramos para este parceiro
                        latestPedidoMap[partnerId] = {
                            latest_date: rec.created_at,
                            ids: [rec.id_pedido] // Armazena como um array
                        };
                    } else if (latestPedidoMap[partnerId].latest_date === rec.created_at) {
                        // Se a data for EXATAMENTE a mesma, adiciona ao array (trata empates)
                        latestPedidoMap[partnerId].ids.push(rec.id_pedido);
                    }
                    // Se for uma data mais antiga, é ignorado pois já temos o mais recente.
                });
            }
        }

        // 4. Mapear os dados para as linhas da tabela (agora síncrono, pois já buscamos os dados)
        const rows = data.sort((a, b) => a.parceiro.localeCompare(b.parceiro)).map(p => {
            const arquiteto = this.arquitetos.find(arq => arq.id === p.id_parceiro);
            const chavePix = arquiteto ? `${arquiteto.tipo_chave_pix || ''} ${arquiteto.pix || 'Não cadastrada'}`.trim() : 'Não encontrado';
            
            // Busca a informação do pedido no mapa que criamos
            const pedidoInfo = latestPedidoMap[p.id_parceiro];
            // Junta os IDs (caso haja mais de um por empate) com vírgula
            const idPedidoHtml = pedidoInfo ? pedidoInfo.ids.join(', ') : 'N/A'; 

            return `
            <tr class="border-b">
                <td class="p-2">${p.id_parceiro}</td>
                <td class="p-2">${p.parceiro}</td>
                <td class="p-2">${chavePix}</td>
                <td class="p-2">${idPedidoHtml}</td> <!-- COLUNA ADICIONADA -->
                <td class="p-2">${p.consultor || ''}</td>
                <td class="p-2 text-right">${formatCurrency(p.rt_valor)}</td>
            </tr>`;
        }).join('');
        
        // 5. Adicionar o cabeçalho da nova coluna no HTML final
        const content = `<div class="report-section">
          <h2 class="text-2xl font-bold mb-6">Relatório de Pagamento - ${date}</h2>
          <table class="w-full text-sm">
            <thead>
              <tr class="border-b-2 border-gray-300">
                <th class="p-2 text-left">ID</th>
                <th class="p-2 text-left">Parceiro</th>
                <th class="p-2 text-left">Chave Pix</th>
                <th class="p-2 text-left">ID Pedido</th> <!-- CABEÇALHO ADICIONADO -->
                <th class="p-2 text-left">Consultor</th>
                <th class="p-2 text-right">Valor RT</th>
              </tr>
            </thead>
            <tbody>${rows}</tbody>
          </table>
        </div>`;
        
        const template = `<html><head><title>Relatório - ${date}</title><script src="https://cdn.tailwindcss.com"><\/script><style>@media print{.no-print{display: none;}} body { font-family: sans-serif; }</style></head><body class="p-8 bg-gray-100"><div class="no-print text-center mb-8"><button onclick="window.print()" class="bg-blue-600 text-white py-2 px-6 rounded-lg shadow-md hover:bg-blue-700 transition">Imprimir</button></div><div class="max-w-5xl mx-auto bg-white p-12 rounded-xl shadow-2xl">${content}<div class="mt-12 text-right border-t-2 pt-6"><h3 class="text-xl font-bold text-gray-700">Soma Total (RT) a Pagar</h3><p class="text-4xl font-bold mt-2 text-green-600">${formatCurrency(total)}</p></div></div></body></html>`;
        const win = window.open('', '_blank');
        win.document.write(template);
        win.document.close();
    }

    async handleGerarPagamentosClick() {
        if (!this.schemaHasRtAcumulado || !this.schemaHasRtTotalPago) { alert("Funcionalidade desabilitada. Verifique o console."); return; }
        const { data, error } = await supabase.from('arquitetos').select('*');
        if (error) { alert("Não foi possível buscar dados atualizados."); return; }
        this.arquitetos = data || [];
        this.eligibleForPayment = this.arquitetos.filter(a => parseFloat(a.rt_acumulado || 0) >= this.minPaymentValue);
        if (this.eligibleForPayment.length === 0) { alert(`Nenhum arquiteto atingiu o valor mínimo para pagamento (${formatCurrency(this.minPaymentValue)}).`); return; }
        this.openGerarPagamentosModal();
    }

    async confirmarGeracaoComprovantes() {
        if (this.eligibleForPayment.length === 0) return;

        const partnerIds = this.eligibleForPayment.map(a => a.id);
        const { data: consultantData, error: consultantError } = await supabase
            .from('sysled_imports')
            .select('id_parceiro, consultor, data_finalizacao_prevenda')
            .in('id_parceiro', partnerIds)
            .order('data_finalizacao_prevenda', { ascending: false });

        if (consultantError) {
            alert("Erro ao buscar dados do consultor: " + consultantError.message);
            return;
        }

        const consultantMap = {};
        if (consultantData) {
            for (const record of consultantData) {
                if (!consultantMap[record.id_parceiro]) {
                    consultantMap[record.id_parceiro] = record.consultor;
                }
            }
        }

        const todayDB = new Date().toISOString().slice(0, 10);
        const pagamentos = this.eligibleForPayment.map(a => ({
            id_parceiro: a.id,
            parceiro: a.nome,
            rt_valor: a.rt_acumulado,
            pago: false,
            data_geracao: todayDB,
            consultor: consultantMap[a.id] || null,
            form_pagamento: 1 // Pagamento Comum
        }));

        const { error: insertError } = await supabase.from('pagamentos').insert(pagamentos);
        if (insertError) { alert("Erro ao gerar comprovantes: " + insertError.message); return; }

        const updates = this.eligibleForPayment.map(a =>
            supabase.from('arquitetos').update({
                rt_acumulado: 0,
                rt_total_pago: (parseFloat(a.rt_total_pago) || 0) + (parseFloat(a.rt_acumulado) || 0)
            }).eq('id', a.id)
        );
        await Promise.all(updates);

        alert(`${this.eligibleForPayment.length} comprovantes gerados!`);
        await this.logAction(`Gerou ${this.eligibleForPayment.length} pagamentos em lote.`);
        this.closeGerarPagamentosModal();
        this.eligibleForPayment = [];
        await this.loadData();
        this.renderAll();
        document.querySelector('.menu-link[data-tab="comprovantes"]').click();
    }

    async handleGerarPagamentoFicha() {
        const id = document.getElementById('edit-arquiteto-original-id').value;
        const arq = this.arquitetos.find(a => a.id === id);
        if (!arq) return;
        const valor = parseFloat(arq.rt_acumulado || 0);
        if (valor <= 0) { alert('Arquiteto sem saldo de RT acumulado.'); return; }
        if (confirm(`Gerar pagamento de ${formatCurrency(valor)} para ${arq.nome}? O saldo será zerado.`)) {
            const { data: latestImport, error: consultantError } = await supabase
                .from('sysled_imports')
                .select('consultor')
                .eq('id_parceiro', arq.id)
                .order('data_finalizacao_prevenda', { ascending: false })
                .limit(1)
                .single();
            
            if (consultantError && consultantError.code !== 'PGRST116') {
                console.error("Aviso: Não foi possível encontrar o consultor. O pagamento será gerado sem essa informação.", consultantError);
            }
            const consultantName = latestImport ? latestImport.consultor : null;

            const todayDB = new Date().toISOString().slice(0, 10);
            const { error: insertError } = await supabase.from('pagamentos').insert([{
                id_parceiro: arq.id,
                parceiro: arq.nome,
                rt_valor: valor,
                pago: false,
                data_geracao: todayDB,
                consultor: consultantName,
                form_pagamento: 1 // Pagamento comum
            }]);

            if (insertError) { alert("Erro ao gerar comprovante: " + insertError.message); return; }

            const { error: updateError } = await supabase.from('arquitetos').update({ rt_acumulado: 0, rt_total_pago: (parseFloat(arq.rt_total_pago) || 0) + valor }).eq('id', arq.id);
            if (updateError) alert("Comprovante gerado, mas erro ao atualizar saldo: " + updateError.message);
            else {
                alert(`Comprovante gerado com sucesso para ${arq.nome}!`);
                await this.logAction(`Gerou pagamento individual de ${formatCurrency(valor)} para ${arq.nome} (ID: ${id}).`);
            }
            this.closeEditModal();
            await this.loadData();
            this.renderAll();
            document.querySelector('.menu-link[data-tab="comprovantes"]').click();
        }
    }

    /**
     * CORRIGIDO: Gera um resgate a partir da ficha do arquiteto, similar ao Gerar Pagamento.
     */
    async handleGerarResgateFicha() {
        console.log("Iniciando handleGerarResgateFicha..."); 

        const id = document.getElementById('edit-arquiteto-original-id').value;
        const arq = this.arquitetos.find(a => a.id === id);

        if (!arq) {
            console.error("handleGerarResgateFicha: Arquiteto não encontrado com o ID:", id); 
            alert('Erro: Arquiteto não encontrado.');
            return;
        }
        console.log("handleGerarResgateFicha: Arquiteto encontrado:", arq);

        const valor = parseFloat(arq.rt_acumulado || 0);
        console.log("handleGerarResgateFicha: Valor do resgate:", valor); 

        if (valor <= 0) {
            alert('Arquiteto sem saldo de RT acumulado para resgate.');
            return;
        }

        if (confirm(`Gerar resgate de ${formatCurrency(valor)} para ${arq.nome}? O saldo será zerado.`)) {
            console.log("handleGerarResgateFicha: Usuário confirmou o resgate."); 

            // Busca o último consultor associado a uma venda para este parceiro
            const { data: latestImport, error: consultantError } = await supabase
                .from('sysled_imports')
                .select('consultor')
                .eq('id_parceiro', arq.id)
                .order('data_finalizacao_prevenda', { ascending: false })
                .limit(1)
                .single();
            
            if (consultantError && consultantError.code !== 'PGRST116') { // PGRST116 = no rows found, which is ok
                console.error("handleGerarResgateFicha: Erro ao buscar consultor:", consultantError);
            }
            const consultantName = latestImport ? latestImport.consultor : null;
            console.log("handleGerarResgateFicha: Consultor encontrado:", consultantName); 

            const todayDB = new Date().toISOString().slice(0, 10);
            
            const payload = {
                id_parceiro: arq.id,
                parceiro: arq.nome,
                rt_valor: valor,
                pago: false,
                data_geracao: todayDB,
                consultor: consultantName,
                form_pagamento: 2 // Identifica o registro como um RESGATE
            };
            console.log("handleGerarResgateFicha: Enviando payload para Supabase:", payload); 

            // Insere o novo registro na tabela 'pagamentos' com form_pagamento = 2
            const { error: insertError } = await supabase.from('pagamentos').insert([payload]);

            if (insertError) {
                console.error("handleGerarResgateFicha: Erro ao inserir resgate no Supabase:", insertError);
                alert("Erro ao gerar resgate: " + insertError.message);
                return;
            }
            console.log("handleGerarResgateFicha: Resgate inserido com sucesso.");

            // Zera o saldo acumulado e atualiza o total pago do arquiteto
            const updatePayload = {
                rt_acumulado: 0,
                rt_total_pago: (parseFloat(arq.rt_total_pago) || 0) + valor
            };
            console.log("handleGerarResgateFicha: Atualizando arquiteto com payload:", updatePayload);
            
            const { error: updateError } = await supabase.from('arquitetos').update(updatePayload).eq('id', arq.id);

            if (updateError) {
                console.error("handleGerarResgateFicha: Erro ao atualizar saldo do arquiteto:", updateError);
                alert("Resgate gerado, mas erro ao atualizar saldo do arquiteto: " + updateError.message);
            } else {
                console.log("handleGerarResgateFicha: Saldo do arquiteto atualizado com sucesso.");
                alert(`Resgate de ${formatCurrency(valor)} gerado com sucesso para ${arq.nome}!`);
                await this.logAction(`Gerou resgate individual de ${formatCurrency(valor)} para ${arq.nome} (ID: ${id}).`);
            }

            this.closeEditModal();
            await this.loadData();
            this.renderAll();
            document.querySelector('.menu-link[data-tab="resgates"]').click();
        } else {
             console.log("handleGerarResgateFicha: Usuário cancelou o resgate.");
        }
    }
    
    async fetchSysledData() {
        const authToken = this.sysledAuthToken;

        const container = document.getElementById('sysled-table-container');

        if (!authToken) {
            alert('Chave da API não configurada no sistema.');
            if (container) container.innerHTML = `<p class="text-center text-red-400 py-8">Chave da API não configurada.</p>`;
            return;
        }

        container.innerHTML = `<p class="text-center text-gray-400 py-8">Buscando dados... <span class="material-symbols-outlined animate-spin align-middle">progress_activity</span></p>`;
        try {
            const response = await fetch(this.sysledApiUrl, { headers: { 'Authorization': authToken } });
            if (!response.ok) {
                if (response.status === 401 || response.status === 403) {
                    throw new Error(`Erro de autorização (${response.status}). Verifique se a Chave da API está correta.`);
                }
                throw new Error(`Erro na API: ${response.statusText} (${response.status})`);
            }
            this.sysledData = await response.json();
            await this.logAction("Atualizou os dados da consulta Sysled.");
        } catch (error) {
            console.error("Erro na API Sysled:", error);
            container.innerHTML = `<p class="text-center text-red-400 py-8">${error.message}</p>`;
        } finally {
            this.renderSysledTable();
        }
    }

    clearSysledFilters() {
        document.getElementById('sysled-filter-data-inicio').value = '';
        document.getElementById('sysled-filter-data-fim').value = '';
        document.getElementById('sysled-filter-parceiro').value = '';
        document.getElementById('sysled-filter-excluir-parceiro').value = '';
        this.renderSysledTable();
    }

    async handleCopyToRTClick() {
        if (this.sysledFilteredData.length === 0) {
            alert('Não há dados filtrados para copiar. Por favor, filtre os dados primeiro ou atualize a consulta.');
            return;
        }
    
        if (!confirm(`Você está prestes a importar ${this.sysledFilteredData.length} venda(s) da Sysled. Deseja continuar?`)) {
            return;
        }
    
        this.isSysledImport = true;
        this.tempRTData = this.sysledFilteredData;
    
        const mapping = {
            id_prevenda: 'idPedido', data_venda: 'dataFinalizacaoPrevenda', nome_cliente: 'clienteFantasia',
            valor_venda: 'valorNota', executivo: 'consultor', id_parceiro: 'idParceiro',
            parceiro: 'parceiro', loja: 'idEmpresa'
        };
    
        const firstRow = this.tempRTData[0];
        if (!firstRow.hasOwnProperty(mapping.id_parceiro) || !firstRow.hasOwnProperty(mapping.valor_venda) || !firstRow.hasOwnProperty(mapping.id_prevenda)) {
            alert("Os dados da Sysled parecem estar incompletos. Colunas essenciais como 'idParceiro', 'valorNota' ou 'idPedido' não foram encontradas. Importação cancelada.");
            this.isSysledImport = false;
            return;
        }
    
        let processedData = this.tempRTData.map(row => {
            const newRow = {};
            for (const key in mapping) {
                if (row.hasOwnProperty(mapping[key])) {
                    if (key === 'valor_venda') {
                        newRow[key] = parseApiNumber(row[mapping[key]]);
                    } else {
                        newRow[key] = row[mapping[key]];
                    }
                }
            }
            return newRow;
        });
    
        // Verificar se existem arquitetos não cadastrados
        const arquitetosNaoCadastrados = await this.verificarArquitetosNaoCadastrados(processedData);
        if (arquitetosNaoCadastrados.length > 0) {
            this.pendingImportData = processedData;
            this.showNovoArquitetoModal(arquitetosNaoCadastrados[0]);
            return;
        }
    
        let dataToProcess = processedData;
    
        const pedidoIds = processedData.map(row => row.id_prevenda).filter(id => id);
        if (pedidoIds.length > 0) {
            const { data: existing, error } = await supabase.from('sysled_imports').select('id_pedido').in('id_pedido', pedidoIds);
    
            if (error) {
                alert('Erro ao verificar vendas existentes: ' + error.message);
                this.isSysledImport = false;
                return;
            }
    
            const existingIds = new Set(existing.map(item => String(item.id_pedido)));
            const alreadyImported = processedData.filter(row => existingIds.has(String(row.id_prevenda)));
            dataToProcess = processedData.filter(row => !existingIds.has(String(row.id_prevenda)));
    
            if (alreadyImported.length > 0) {
                alert(`Venda(s) já importada(s) e ignorada(s): ${alreadyImported.map(r => r.id_prevenda).join(', ')}`);
            }
        }
    
        if (dataToProcess.length > 0) {
            await this.processRTData(dataToProcess);
        } else {
            alert("Nenhuma venda nova para importar. Todas as vendas filtradas já foram processadas anteriormente.");
            this.isSysledImport = false;
        }
    }
    
    async handleConsultarVendasClick(e) {
        e.preventDefault();
        const id = document.getElementById('edit-arquiteto-original-id').value;
        if (!id) return;
        const arq = this.arquitetos.find(a => a.id === id);
        document.getElementById('sales-history-modal-title').textContent = `Histórico de Vendas para ${arq ? arq.nome : id}`;
        const container = document.getElementById('sales-history-table-container');
        container.innerHTML = `<p class="text-center text-gray-400 py-8">Consultando... <span class="material-symbols-outlined animate-spin align-middle">progress_activity</span></p>`;
        const modal = document.getElementById('sales-history-modal');
        modal.onclick = (e) => {
            if (e.target === modal) this.closeSalesHistoryModal();
        };
        modal.classList.add('active');
        try {
            const { data, error } = await supabase.from('sysled_imports').select('id_pedido, valor_nota, data_finalizacao_prevenda').eq('id_parceiro', id).order('data_finalizacao_prevenda', { ascending: false });
            if (error) throw error;
            this.renderSalesHistoryModal(data, true); // true indica que são dados da API e podem ter detalhes
        } catch (error) {
            container.innerHTML = `<p class="text-center text-red-400 py-8">Erro ao consultar vendas.</p>`;
        }
    }

    handleSalesHistoryTableClick(e) {
        const btn = e.target.closest('.view-sale-details-btn');
        if (btn) { e.preventDefault(); this.openSaleDetailsModal(btn.dataset.pedidoId); }
    }
    
    async handleImportSingleSale(e) {
        const id = e.target.dataset.pedidoId;
        if (!id || id === 'N/A') return;
        const { data: existing } = await supabase.from('sysled_imports').select('id_pedido').eq('id_pedido', id).maybeSingle();
        if (existing) { alert(`Venda ${id} já importada.`); return; }
        const sale = this.sysledData.find(row => String(row.idPedido) === id);
        if (!sale) { alert('Dados da venda não encontrados.'); return; }
        const data = [{ id_parceiro: sale.idParceiro, valor_venda: parseApiNumber(sale.valorNota), parceiro: sale.parceiro }];
        this.isSysledImport = false;
        await this.processRTData(data);
        const { error } = await supabase.from('sysled_imports').insert([{ id_parceiro: sale.idParceiro, valor_nota: parseApiNumber(sale.valorNota), data_finalizacao_prevenda: sale.dataFinalizacaoPrevenda, id_pedido: sale.idPedido }]);
        if (error) console.error("Erro ao registrar importação:", error);
        this.closeSaleDetailsModal();
        this.closeSalesHistoryModal();
    }

    handleSort(e) {
        const header = e.target.closest('.sortable-header');
        if (!header) return;
        const column = header.dataset.sort;
        if (this.sortColumn === column) {
            this.sortDirection = this.sortDirection === 'asc' ? 'desc' : 'asc';
        } else {
            this.sortColumn = column;
            this.sortDirection = 'asc';
        }
        this.renderArquitetosTable();
    }

    calculateRT() {
        const valor = parseCurrency(document.getElementById('rt-valor-vendas').textContent);
        const perc = parseFloat(document.getElementById('rt-percentual').value);
        document.getElementById('rt-valor-calculado').textContent = formatCurrency(valor * perc);
    }

    handleArquivosImportadosClick(e) {
        const btn = e.target.closest('.download-arquivo-btn');
        if (btn) { e.preventDefault(); this.downloadImportedFile(btn.dataset.date); }
    }

    downloadImportedFile(date) {
        const file = this.importedFiles[date];
        if (file) {
            const link = document.createElement('a');
            link.href = file.dataUrl;
            link.download = file.name;
            document.body.appendChild(link);
            link.click();
            document.body.removeChild(link);
        }
    }

    async handleAddComissaoManual(e) {
        e.preventDefault();
        const form = e.target;
        const idParceiro = document.getElementById('manual-id-parceiro').value.trim();
        const idVenda = document.getElementById('manual-id-venda').value.trim();
        const valorVenda = parseFloat(document.getElementById('manual-valor-venda').value);

        if (!idParceiro || isNaN(valorVenda) || valorVenda <= 0) {
            alert('Preencha o ID do Parceiro e um Valor de Venda válido.');
            return;
        }

        // Verificação de duplicidade ANTES de criar a solicitação
        if (idVenda) {
            // 1. Checa se já foi importado via Sysled
            const { data: existingImport, error: importError } = await supabase
                .from('sysled_imports')
                .select('id_pedido')
                .eq('id_pedido', idVenda)
                .maybeSingle();
            
            if (importError) {
                alert('Erro ao verificar duplicidade de importação: ' + importError.message);
                return;
            }
            if (existingImport) {
                alert(`Venda com ID ${idVenda} já foi importada anteriormente e não pode ser incluída manualmente.`);
                return;
            }

            // 2. Checa se já existe uma comissão manual com o mesmo ID de venda
            const { data: existingManual, error: manualError } = await supabase
                .from('comissoes_manuais')
                .select('id')
                .eq('id_venda', idVenda)
                .maybeSingle();
            
            if (manualError) {
                alert('Erro ao verificar duplicidade de comissão manual: ' + manualError.message);
                return;
            }
            if (existingManual) {
                alert(`Já existe uma inclusão manual para a venda com ID ${idVenda}.`);
                return;
            }
        }

        const arq = this.arquitetos.find(a => a.id === idParceiro);
        if (!arq) {
            alert(`Arquiteto com ID ${idParceiro} não encontrado.`);
            return;
        }

        const newComissao = {
            id_parceiro: idParceiro,
            id_venda: idVenda,
            valor_venda: valorVenda,
            data_venda: document.getElementById('manual-data-venda').value,
            consultor: document.getElementById('manual-consultor').value,
            justificativa: document.getElementById('manual-justificativa').value,
            status: 'pendente'
        };

        const { error } = await supabase.from('comissoes_manuais').insert(newComissao);

        if (error) {
            alert("Erro ao salvar solicitação: " + error.message);
            return;
        }

        alert('Solicitação de comissão manual enviada para aprovação!');
        await this.logAction(`Enviou comissão manual para aprovação de ${formatCurrency(valorVenda)} para ${arq.nome}`);
        form.reset();
        await this.loadData();
        this.renderAll();
    }
    
    async handleAprovarInclusaoManual(e) {
        const btn = e.target.closest('#aprovar-inclusao-manual-btn');
        if (!btn) return;
    
        const comissaoId = parseInt(btn.dataset.comissaoId, 10);
        const comissao = this.comissoesManuais.find(c => c.id === comissaoId);
        if (!comissao) {
            alert('Erro: Comissão não encontrada.'); return;
        }
    
        if (comissao.status === 'aprovada') {
            alert('Esta comissão já foi aprovada.'); return;
        }

        // Verifica se a venda já foi importada antes de aprovar
        if (comissao.id_venda) {
            const { data: existingSale, error: checkError } = await supabase
                .from('sysled_imports')
                .select('id_pedido')
                .eq('id_pedido', comissao.id_venda)
                .maybeSingle();

            if (checkError) {
                alert('Erro ao verificar a existência da venda: ' + checkError.message);
                return;
            }

            if (existingSale) {
                alert(`Não é possível aprovar. A venda com ID ${comissao.id_venda} já foi importada anteriormente.`);
                return;
            }
        }
    
        if (!confirm(`Aprovar a inclusão de ${formatCurrency(comissao.valor_venda)} para o parceiro ${comissao.id_parceiro}?`)) return;
    
        const arq = this.arquitetos.find(a => a.id === comissao.id_parceiro);
        if (!arq) {
            alert(`Arquiteto com ID ${comissao.id_parceiro} não foi encontrado.`); return;
        }
    
        const valorVenda = comissao.valor_venda;
        const payload = {
            valorVendasTotal: (arq.valorVendasTotal || 0) + valorVenda,
            pontos: (this.pontuacoes[comissao.id_parceiro] || 0) + Math.floor(valorVenda / 1000),
            salesCount: (arq.salesCount || 0) + 1,
        };
        if (this.schemaHasRtAcumulado) {
            payload.rt_acumulado = parseFloat(arq.rt_acumulado || 0) + (valorVenda * (arq.rtPercentual || 0.05));
        }
    
        const { error: updateError } = await supabase.from('arquitetos').update(payload).eq('id', comissao.id_parceiro);
        if (updateError) {
            alert("Erro ao atualizar dados do arquiteto: " + updateError.message); return;
        }
    
        const { error: comissaoError } = await supabase.from('comissoes_manuais').update({ status: 'aprovada' }).eq('id', comissaoId);
        if (comissaoError) {
            alert("Dados do arquiteto atualizados, mas falha ao marcar comissão como aprovada: " + comissaoError.message);
        }
    
        if (comissao.id_venda) {
            const { error } = await supabase.from('sysled_imports').insert({ 
                id_parceiro: comissao.id_parceiro, 
                valor_nota: comissao.valor_venda, 
                data_finalizacao_prevenda: comissao.data_venda, 
                id_pedido: comissao.id_venda,
                consultor: comissao.consultor // Adicionado o campo consultor
            });
            if (error) alert("Aviso: Erro ao registrar na tabela de controle de duplicados (sysled_imports).");
        }
        
        alert('Comissão aprovada e valores contabilizados com sucesso!');
        await this.logAction(`Aprovou comissão manual de ${formatCurrency(valorVenda)} para ${arq.nome} (ID: ${arq.id})`);
        
        this.closeComissaoManualDetailsModal();
        await this.loadData();
        this.renderAll();
    }

    handleHistoricoManualClick(e) {
        const btn = e.target.closest('.view-comissao-details-btn');
        if (btn) { e.preventDefault(); this.openComissaoManualDetailsModal(parseInt(btn.dataset.comissaoId, 10)); }
    }

    // Função para verificar arquitetos não cadastrados
    async verificarArquitetosNaoCadastrados(processedData) {
        const arquitetosIds = [...new Set(processedData.map(row => String(row.id_parceiro)))];
        const arquitetosNaoCadastrados = [];
        
        for (const id of arquitetosIds) {
            const arquiteto = this.arquitetos.find(a => a.id === id);
            if (!arquiteto) {
                const vendaExemplo = processedData.find(row => String(row.id_parceiro) === id);
                arquitetosNaoCadastrados.push({
                    id: id,
                    nome: vendaExemplo.parceiro || 'Novo Parceiro'
                });
            }
        }
        
        return arquitetosNaoCadastrados;
    }

    // Mostra o modal de cadastro de novo arquiteto
    showNovoArquitetoModal(arquiteto) {
        document.getElementById('novo-arquiteto-id').value = arquiteto.id;
        document.getElementById('novo-arquiteto-nome').value = arquiteto.nome;
        document.getElementById('novo-arquiteto-id-display').value = arquiteto.id;
        document.getElementById('novo-arquiteto-nome-display').value = arquiteto.nome;
        
        // Limpar campos
        document.getElementById('novo-arquiteto-email').value = '';
        document.getElementById('novo-arquiteto-tipo-pix').value = '';
        document.getElementById('novo-arquiteto-pix').value = '';
        document.getElementById('novo-arquiteto-telefone').value = '';
        
        const modal = document.getElementById('novo-arquiteto-modal');
        modal.onclick = (e) => {
            if (e.target === modal) this.cancelNovoArquiteto();
        };
        modal.classList.add('active');
    }

    // Cancela o cadastro de novo arquiteto
    cancelNovoArquiteto() {
        document.getElementById('novo-arquiteto-modal').classList.remove('active');
        this.isSysledImport = false;
        this.pendingImportData = null;
    }

    // Processa o cadastro de novo arquiteto
    async handleNovoArquitetoSubmit(e) {
        e.preventDefault();
        
        const id = document.getElementById('novo-arquiteto-id').value;
        const nome = document.getElementById('novo-arquiteto-nome').value;
        const email = document.getElementById('novo-arquiteto-email').value;
        const tipoPix = document.getElementById('novo-arquiteto-tipo-pix').value;
        const pix = document.getElementById('novo-arquiteto-pix').value;
        const telefone = document.getElementById('novo-arquiteto-telefone').value;
        
        if (!email || !pix) {
            alert('E-mail e Chave PIX são obrigatórios.');
            return;
        }
        
        try {
            const novoArquitetoData = {
                id: id,
                nome: nome,
                email: email,
                telefone: telefone,
                tipo_chave_pix: tipoPix,
                pix: pix,
                salesCount: 0,
                valorVendasTotal: 0,
                pontos: 0,
                rtPercentual: 0.05,
                rt_acumulado: 0,
                rt_total_pago: 0
            };
            
            const { data: created, error } = await supabase.from('arquitetos').insert(novoArquitetoData).select().single();
            
            if (error) {
                alert('Erro ao cadastrar arquiteto: ' + error.message);
                return;
            }
            
            // Adicionar à lista local
            this.arquitetos.push(created);
            this.pontuacoes[id] = 0;
            
            await this.logAction(`Cadastrou novo arquiteto: ${nome} (ID: ${id})`);
            
            // Fechar modal
            document.getElementById('novo-arquiteto-modal').classList.remove('active');
            
            // Continuar com a importação
            if (this.pendingImportData) {
                await this.continuarImportacao();
            }
            
        } catch (error) {
            alert('Erro ao cadastrar arquiteto: ' + error.message);
        }
    }

    // Continua a importação após cadastrar o arquiteto
    async continuarImportacao() {
        if (!this.pendingImportData) return;
        
        const dataToProcess = this.pendingImportData;
        this.pendingImportData = null;
        
        const pedidoIds = dataToProcess.map(row => row.id_prevenda).filter(id => id);
        if (pedidoIds.length > 0) {
            const { data: existing, error } = await supabase.from('sysled_imports').select('id_pedido').in('id_pedido', pedidoIds);
    
            if (error) {
                alert('Erro ao verificar vendas existentes: ' + error.message);
                this.isSysledImport = false;
                return;
            }
    
            const existingIds = new Set(existing.map(item => String(item.id_pedido)));
            const alreadyImported = dataToProcess.filter(row => existingIds.has(String(row.id_prevenda)));
            const newDataToProcess = dataToProcess.filter(row => !existingIds.has(String(row.id_prevenda)));
    
            if (alreadyImported.length > 0) {
                alert(`Venda(s) já importada(s) e ignorada(s): ${alreadyImported.map(r => r.id_prevenda).join(', ')}`);
            }
            
            if (newDataToProcess.length > 0) {
                await this.processRTData(newDataToProcess);
            } else {
                alert("Nenhuma venda nova para importar. Todas as vendas filtradas já foram processadas anteriormente.");
                this.isSysledImport = false;
            }
        } else {
            await this.processRTData(dataToProcess);
        }
    }

    /**
     * NOVO: Atualiza a permissão (role) de um usuário.
     */
    async handleUpdateUserPermission(e) {
        const btn = e.target.closest('.save-permission-btn');
        if (!btn) return;

        const userId = btn.dataset.userId;
        const select = document.querySelector(`select[data-user-id="${userId}"]`);
        const newRole = select.value;
        const userEmail = this.users.find(u => u.id === userId)?.email || 'ID ' + userId;

        const { error } = await supabase.from('profiles').update({ role: newRole }).eq('id', userId);

        if (error) {
            alert(`Erro ao atualizar permissão para ${userEmail}: ${error.message}`);
        } else {
            alert(`Permissão de ${userEmail} atualizada para '${newRole}'.`);
            await this.logAction(`Alterou a permissão de ${userEmail} para ${newRole}.`);
            await this.loadData();
            this.renderPermissionsTab();
        }
    }

    /**
     * Calcula os KPIs do parceiro com base nos dados brutos da API Sysled (Memória apenas).
     */
    /**
     * Calcula os KPIs do parceiro com base nos dados brutos da API Sysled (Memória apenas).
     * Lógica de Saúde atualizada para considerar apenas o MÊS ATUAL.
     */
    /**
     * Calcula os KPIs do parceiro com base nos dados brutos da API Sysled (Memória apenas).
     * Lógica de Saúde atualizada para considerar apenas o MÊS ATUAL.
     * AGORA: As colunas de Projetos também retornam apenas os dados do mês para a conta bater.
     */
    calculatePartnerKPIs(partnerId, apiData) {
        // Se a API ainda não foi carregada, retorna traços
        if (!apiData || apiData.length === 0) {
            return {
                saude_carteira: '-',
                tempo_sem_envio: '-',
                projeto_fechado: '-',
                projeto_enviado: '-',
                data_envio: null,
                data_fechamento: null
            };
        }

        // 1. Filtra todos os registros da API para este parceiro
        const partnerRecords = apiData.filter(row => String(row.idParceiro) === String(partnerId));

        if (partnerRecords.length === 0) {
            return {
                saude_carteira: '0%',
                tempo_sem_envio: 'N/A',
                projeto_fechado: 0,
                projeto_enviado: 0,
                data_envio: null,
                data_fechamento: null
            };
        }

        // --- DEFINIÇÃO DO MÊS ATUAL ---
        const hoje = new Date();
        const mesAtual = hoje.getMonth(); // 0 a 11
        const anoAtual = hoje.getFullYear();

        // Helper para verificar se uma data string (YYYY-MM-DD) cai no mês atual
        const isMesAtual = (dateString) => {
            if (!dateString) return false;
            const d = new Date(dateString + 'T12:00:00');
            return d.getMonth() === mesAtual && d.getFullYear() === anoAtual;
        };

        // --- CÁLCULO DA SAÚDE E TOTAIS (APENAS MÊS ATUAL) ---
        
        // Fechados no Mês
        const fechadosMes = partnerRecords.filter(p => 
            String(p.pedidoStatus) === '9' && 
            isMesAtual(p.dataFinalizacaoPrevenda)
        ).length;

        // Enviados no Mês
        const enviadosMes = partnerRecords.filter(p => 
            (p.versaoPedido === null || p.versaoPedido === 'null' || p.versaoPedido === '') && 
            isMesAtual(p.dataEmissaoPrevenda)
        ).length;

        // Saúde Carteira
        let saude = 0;
        if (enviadosMes > 0) {
            saude = (fechadosMes / enviadosMes) * 100;
        }

        // Explicação do 108%: Se o arquiteto enviou 10 projetos ESTE mês, mas fechou 11 (alguns eram do mês passado), a saúde será 110%.

        // --- DATAS E TEMPO SEM ENVIO (MANTIDO) ---
        const datasEnvio = partnerRecords
            .map(p => p.dataEmissaoPrevenda)
            .filter(d => d)
            .sort((a, b) => new Date(b) - new Date(a));
        const lastDataEnvio = datasEnvio.length > 0 ? datasEnvio[0] : null;

        const datasFechamento = partnerRecords
            .map(p => p.dataFinalizacaoPrevenda)
            .filter(d => d)
            .sort((a, b) => new Date(b) - new Date(a));
        const lastDataFechamento = datasFechamento.length > 0 ? datasFechamento[0] : null;

        let diasSemEnvio = '-';
        if (lastDataEnvio) {
            const ultimoEnvio = new Date(lastDataEnvio + 'T12:00:00');
            const diffTime = Math.abs(hoje - ultimoEnvio);
            diasSemEnvio = Math.ceil(diffTime / (1000 * 60 * 60 * 24)) + ' dias'; 
        }

        return {
            saude_carteira: saude.toFixed(1) + '%',
            
            // ALTERAÇÃO AQUI: Agora mostramos as contagens DO MÊS, para bater com a porcentagem
            projeto_fechado: fechadosMes, 
            projeto_enviado: enviadosMes,
            
            tempo_sem_envio: diasSemEnvio,
            data_envio: lastDataEnvio,
            data_fechamento: lastDataFechamento
        };
    }

    /**
     * Renderiza a aba de Carteira com Design Premium, Ordenação e Auto-Load.
     */
    async renderCarteiraTab() {
        const container = document.getElementById('carteira-container');
        if (!container) return;

        // Injeta os modais se não existirem
        if (!document.getElementById('carteira-mapping-modal')) this.injectCarteiraModal();
        if (!document.getElementById('carteira-manual-modal')) this.injectCarteiraManualModal();

        // Verifica se já temos dados da API carregados em memória
        const hasApiData = this.sysledData && this.sysledData.length > 0;

        // 1. PREPARAÇÃO DOS DADOS (Se houver dados, prepara para exibição, senão array vazio)
        let combinedData = [];
        if (hasApiData) {
            combinedData = this.carteira.map(c => {
                const arq = this.arquitetos.find(a => String(a.id) === String(c.id_parceiro));
                const kpis = this.calculatePartnerKPIs(c.id_parceiro, this.sysledData);

                return {
                    ...c,
                    vendas: arq ? arq.valorVendasTotal : 0,
                    comissoes: arq ? arq.rt_acumulado : 0,
                    ...kpis
                };
            });

            // 2. LÓGICA DE ORDENAÇÃO
            if (this.carteiraSortColumn) {
                combinedData.sort((a, b) => {
                    let valA = a[this.carteiraSortColumn];
                    let valB = b[this.carteiraSortColumn];

                    if (this.carteiraSortColumn === 'tempo_sem_envio') {
                        const parseDays = (val) => {
                            if (!val || val === '-' || val === 'N/A') return -1;
                            return parseInt(val.replace(/\D/g, '')) || 0;
                        };
                        valA = parseDays(valA);
                        valB = parseDays(valB);
                    } 
                    else if (this.carteiraSortColumn === 'saude_carteira') {
                        valA = parseFloat(valA) || 0;
                        valB = parseFloat(valB) || 0;
                    }

                    if (valA < valB) return this.carteiraSortDirection === 'asc' ? -1 : 1;
                    if (valA > valB) return this.carteiraSortDirection === 'asc' ? 1 : -1;
                    return 0;
                });
            }
        }

        // 3. RENDERIZAÇÃO (DESIGN PREMIUM FIXO)
        const getSortIcon = (col) => {
            if (this.carteiraSortColumn !== col) return '<span class="material-symbols-outlined text-xs text-gray-600 align-middle ml-1">unfold_more</span>';
            return this.carteiraSortDirection === 'asc' 
                ? '<span class="material-symbols-outlined text-xs text-primary align-middle ml-1">expand_less</span>' 
                : '<span class="material-symbols-outlined text-xs text-primary align-middle ml-1">expand_more</span>';
        };

        const controlsHtml = `
            <div class="flex flex-wrap justify-between items-center mb-6 gap-4">
                <h2 class="text-2xl font-bold text-white tracking-tight">Carteira de Parceiros</h2>
                <div class="flex items-center gap-3">
                    <button id="btn-refresh-carteira" class="flex items-center gap-2 py-2.5 px-5 bg-blue-600 hover:bg-blue-500 text-white rounded-lg text-sm font-medium transition-all shadow-lg hover:shadow-blue-500/20 active:scale-95">
                        <span class="material-symbols-outlined text-lg">refresh</span>Atualizar
                    </button>
                    <button id="btn-open-carteira-manual" class="flex items-center gap-2 py-2.5 px-5 bg-teal-600 hover:bg-teal-500 text-white rounded-lg text-sm font-medium transition-all shadow-lg hover:shadow-teal-500/20 active:scale-95">
                        <span class="material-symbols-outlined text-lg">add_circle</span>Manual
                    </button>
                    <label for="carteira-file-input" class="flex items-center gap-2 py-2.5 px-5 bg-indigo-600 hover:bg-indigo-500 text-white rounded-lg cursor-pointer text-sm font-medium transition-all shadow-lg hover:shadow-indigo-500/20 active:scale-95">
                        <span class="material-symbols-outlined text-lg">upload_file</span>Importar
                    </label>
                    <input type="file" id="carteira-file-input" class="hidden" accept=".xlsx, .xls">
                </div>
            </div>
            
            <div id="carteira-loading-status" class="hidden mb-6 p-4 bg-gray-800/50 border border-gray-700 rounded-xl backdrop-blur-md">
                <div class="flex items-center justify-between mb-2">
                    <div class="flex items-center gap-3">
                        <span class="material-symbols-outlined animate-spin text-blue-400">progress_activity</span>
                        <span id="carteira-loading-text" class="text-gray-200 font-medium">Processando...</span>
                    </div>
                    <span id="carteira-counter-text" class="text-sm text-blue-300 font-mono font-bold">0/${this.carteira.length}</span>
                </div>
                <div class="w-full bg-gray-700 h-1.5 rounded-full overflow-hidden">
                    <div id="carteira-progress-bar" class="bg-blue-500 h-full rounded-full transition-all duration-100 shadow-[0_0_10px_rgba(59,130,246,0.5)]" style="width: 0%"></div>
                </div>
            </div>
        `;

        // Gera as linhas SE tiver dados, senão deixa vazio (será preenchido pelo loader ou mensagem vazia)
        const rows = combinedData.map(item => this.createCarteiraRow(item)).join(''); // Usa o helper createCarteiraRow que criamos antes

        // Tabela com Design Fixo (Premium)
        const tableHtml = `
            <div class="glass-card rounded-xl p-0 overflow-hidden border border-white/10 shadow-2xl">
                <div class="overflow-x-auto max-h-[70vh]">
                    <table class="w-full text-left border-collapse">
                        <thead class="sticky top-0 z-20 bg-[#1a1f2e] shadow-md">
                            <tr>
                                <th class="py-4 px-4 font-semibold text-gray-400 text-xs uppercase tracking-wider cursor-pointer hover:text-white transition-colors sort-trigger" data-col="id_parceiro">ID ${getSortIcon('id_parceiro')}</th>
                                <th class="py-4 px-4 font-semibold text-gray-400 text-xs uppercase tracking-wider cursor-pointer hover:text-white transition-colors sort-trigger" data-col="nome">Nome ${getSortIcon('nome')}</th>
                                <th class="py-4 px-4 text-right font-semibold text-gray-400 text-xs uppercase tracking-wider">Vendas</th>
                                <th class="py-4 px-4 text-right font-semibold text-gray-400 text-xs uppercase tracking-wider">Comissões</th>
                                <th class="py-4 px-4 text-center font-semibold text-gray-400 text-xs uppercase tracking-wider cursor-pointer hover:text-white transition-colors sort-trigger" data-col="saude_carteira">Saúde ${getSortIcon('saude_carteira')}</th>
                                <th class="py-4 px-4 text-center font-semibold text-gray-400 text-xs uppercase tracking-wider cursor-pointer hover:text-white transition-colors sort-trigger" data-col="tempo_sem_envio">Tempo s/ Envio ${getSortIcon('tempo_sem_envio')}</th>
                                <th class="py-4 px-4 text-center font-semibold text-gray-400 text-xs uppercase tracking-wider">Fechados</th>
                                <th class="py-4 px-4 text-center font-semibold text-gray-400 text-xs uppercase tracking-wider">Enviados</th>
                                <th class="py-4 px-4 text-center font-semibold text-gray-400 text-xs uppercase tracking-wider">Dt Envio</th>
                                <th class="py-4 px-4 text-center font-semibold text-gray-400 text-xs uppercase tracking-wider">Dt Fechamento</th>
                                <th class="py-4 px-4 text-center font-semibold text-gray-400 text-xs uppercase tracking-wider">Ações</th>
                            </tr>
                        </thead>
                        <tbody id="carteira-table-body" class="divide-y divide-white/5">
                            ${rows || '<tr><td colspan="11" class="text-center text-gray-400 py-12 text-lg">Aguardando dados...</td></tr>'}
                        </tbody>
                    </table>
                </div>
            </div>
        `;

        container.innerHTML = controlsHtml + tableHtml;

        this.setupCarteiraEventListeners();
        
        // AUTO-LOAD: Se não tiver dados da Sysled e tiver parceiros na carteira, inicia o carregamento automaticamente
        if (!hasApiData && this.carteira.length > 0) {
             await this.loadCarteiraWithProgress();
        }
    }
    /**
     * Gerencia o fluxo de buscar dados e atualizar a tabela linha a linha.
     */
    /**
     * Gerencia o fluxo de buscar dados e atualizar a tabela (VERSÃO OTIMIZADA).
     * Evita travamentos usando Renderização em Lote e Indexação de Dados.
     */
    async loadCarteiraWithProgress() {
        const statusDiv = document.getElementById('carteira-loading-status');
        const statusText = document.getElementById('carteira-loading-text');
        const counterText = document.getElementById('carteira-counter-text');
        const progressBar = document.getElementById('carteira-progress-bar');
        const tbody = document.getElementById('carteira-table-body');

        if (!statusDiv) return;

        // Limpa a tabela e mostra o loading
        tbody.innerHTML = ''; 
        statusDiv.classList.remove('hidden');
        statusText.textContent = "Conectando à API Sysled...";
        progressBar.style.width = '5%';

        try {
            // 1. Busca dados da API se estiver vazia
            if (this.sysledData.length === 0) {
                await this.fetchSysledData();
            }

            if (!this.sysledData || this.sysledData.length === 0) {
                throw new Error("Nenhum dado retornado da API.");
            }

            // --- OTIMIZAÇÃO 1: INDEXAÇÃO ---
            // Transforma o array gigante num Dicionário (Map) para acesso instantâneo
            // Isso evita varrer o array 54 vezes. O acesso cai de O(N) para O(1).
            statusText.textContent = "Indexando dados...";
            const sysledMap = new Map();
            // Agrupa os pedidos por ID do Parceiro
            this.sysledData.forEach(row => {
                const pId = String(row.idParceiro);
                if (!sysledMap.has(pId)) sysledMap.set(pId, []);
                sysledMap.get(pId).push(row);
            });

            // 2. Loop de Processamento
            const total = this.carteira.length;
            let rowsBuffer = ''; // Buffer para guardar o HTML na memória

            if (total === 0) {
                tbody.innerHTML = '<tr><td colspan="11" class="text-center text-gray-400 py-8">Nenhum parceiro na carteira.</td></tr>';
                statusDiv.classList.add('hidden');
                return;
            }

            for (let i = 0; i < total; i++) {
                const parceiro = this.carteira[i];
                const currentCount = i + 1;

                // Atualiza UI de progresso
                const progressPercent = Math.round((currentCount / total) * 100);
                statusText.textContent = `Calculando indicadores...`;
                counterText.textContent = `${currentCount}/${total}`;
                progressBar.style.width = `${progressPercent}%`;

                // Pequena pausa (0ms) apenas para o navegador ter fôlego de pintar a barra de progresso
                if (i % 2 === 0) await new Promise(resolve => setTimeout(resolve, 0));

                // Cálculos
                const arq = this.arquitetos.find(a => String(a.id) === String(parceiro.id_parceiro));
                
                // Pega os dados direto do Mapa Indexado (MUITO RÁPIDO) em vez de filtrar tudo de novo
                const partnerApiData = sysledMap.get(String(parceiro.id_parceiro)) || [];
                
                // Usamos uma versão modificada do calculate que aceita os dados já filtrados
                // Precisamos criar um pequeno adaptador ou passar direto para sua lógica
                const kpis = this.calculateKPIsFromSubset(partnerApiData); 

                const item = {
                    ...parceiro,
                    vendas: arq ? arq.valorVendasTotal : 0,
                    comissoes: arq ? arq.rt_acumulado : 0,
                    ...kpis
                };

                // --- OTIMIZAÇÃO 2: BATCH RENDERING ---
                // Acumula na string em vez de inserir no DOM
                rowsBuffer += this.createCarteiraRow(item, i);
            }

            // 3. INSERÇÃO ÚNICA (DOM REFLOW ÚNICO)
            tbody.innerHTML = rowsBuffer;

            // Finalização
            statusText.textContent = "Concluído!";
            setTimeout(() => {
                statusDiv.classList.add('hidden');
            }, 800);

        } catch (error) {
            console.error(error);
            statusText.textContent = `Erro: ${error.message}`;
            statusDiv.classList.replace('bg-blue-900/30', 'bg-red-900/30');
            statusDiv.classList.replace('border-blue-500/50', 'border-red-500/50');
        }
    }

    /**
     * NOVO MÉTODO AUXILIAR: Calcula KPIs recebendo já o array filtrado do parceiro.
     * Isso evita conflito com o método antigo calculatePartnerKPIs que fazia o filter internamente.
     */
    calculateKPIsFromSubset(partnerRecords) {
        if (!partnerRecords || partnerRecords.length === 0) {
            return {
                saude_carteira: '0%',
                tempo_sem_envio: 'N/A',
                projeto_fechado: 0,
                projeto_enviado: 0,
                data_envio: null,
                data_fechamento: null
            };
        }

        const hoje = new Date();
        const mesAtual = hoje.getMonth();
        const anoAtual = hoje.getFullYear();

        const isMesAtual = (dateString) => {
            if (!dateString) return false;
            const d = new Date(dateString + 'T12:00:00');
            return d.getMonth() === mesAtual && d.getFullYear() === anoAtual;
        };

        // Fechados no Mês
        const fechadosMes = partnerRecords.filter(p => 
            String(p.pedidoStatus) === '9' && 
            isMesAtual(p.dataFinalizacaoPrevenda)
        ).length;

        // Enviados no Mês
        const enviadosMes = partnerRecords.filter(p => 
            (p.versaoPedido === null || p.versaoPedido === 'null' || p.versaoPedido === '') && 
            isMesAtual(p.dataEmissaoPrevenda)
        ).length;

        // Saúde Carteira
        let saude = 0;
        if (enviadosMes > 0) {
            saude = (fechadosMes / enviadosMes) * 100;
        }

        const datasEnvio = partnerRecords
            .map(p => p.dataEmissaoPrevenda)
            .filter(d => d)
            .sort((a, b) => new Date(b) - new Date(a));
        const lastDataEnvio = datasEnvio.length > 0 ? datasEnvio[0] : null;

        const datasFechamento = partnerRecords
            .map(p => p.dataFinalizacaoPrevenda)
            .filter(d => d)
            .sort((a, b) => new Date(b) - new Date(a));
        const lastDataFechamento = datasFechamento.length > 0 ? datasFechamento[0] : null;

        let diasSemEnvio = '-';
        if (lastDataEnvio) {
            const ultimoEnvio = new Date(lastDataEnvio + 'T12:00:00');
            const diffTime = Math.abs(hoje - ultimoEnvio);
            diasSemEnvio = Math.ceil(diffTime / (1000 * 60 * 60 * 24)) + ' dias'; 
        }

        return {
            saude_carteira: saude.toFixed(1) + '%',
            projeto_fechado: fechadosMes, 
            projeto_enviado: enviadosMes,
            tempo_sem_envio: diasSemEnvio,
            data_envio: lastDataEnvio,
            data_fechamento: lastDataFechamento
        };
    }

    /**
     * Helper para gerar o HTML de uma linha da tabela Carteira.
     */
    createCarteiraRow(item, index = 0) {
        // Lógica de cor para a Saúde
        const saudeVal = parseFloat(item.saude_carteira);
        let saudeClass = 'text-gray-500';
        if (saudeVal > 50) saudeClass = 'bg-green-500/20 text-green-400';
        else if (saudeVal > 0) saudeClass = 'bg-yellow-500/20 text-yellow-400';

        // Lógica de cor para Tempo sem Envio
        const dias = parseInt(item.tempo_sem_envio);
        const tempoClass = (item.tempo_sem_envio !== '-' && !isNaN(dias) && dias > 60) ? 'text-red-400 font-bold' : 'text-gray-300';

        return `
            <tr class="group border-b border-white/5 hover:bg-white/5 transition-colors ${index % 2 === 0 ? 'bg-white/[0.02]' : ''} animate-fade-in">
                <td class="py-4 px-4 text-gray-400 text-sm font-mono">${item.id_parceiro}</td>
                <td class="py-4 px-4">
                    <div class="font-medium text-white truncate max-w-[280px]" title="${item.nome}">${item.nome}</div>
                </td>
                <td class="py-4 px-4 text-right text-gray-300 font-medium">${formatCurrency(item.vendas || 0)}</td>
                <td class="py-4 px-4 text-right font-bold text-emerald-400">${formatCurrency(item.comissoes || 0)}</td>
                
                <td class="py-4 px-4 text-center">
                    <span class="inline-block py-1 px-2 rounded text-xs font-bold ${saudeClass}">
                        ${item.saude_carteira}
                    </span>
                </td>
                
                <td class="py-4 px-4 text-center font-medium ${tempoClass}">
                    ${item.tempo_sem_envio}
                </td>
                
                <td class="py-4 px-4 text-center text-gray-300">${item.projeto_fechado}</td>
                <td class="py-4 px-4 text-center text-gray-300">${item.projeto_enviado}</td>
                <td class="py-4 px-4 text-center text-xs text-gray-400">${item.data_envio ? formatApiDateToBR(item.data_envio) : '-'}</td>
                <td class="py-4 px-4 text-center text-xs text-gray-400">${item.data_fechamento ? formatApiDateToBR(item.data_fechamento) : '-'}</td>

                <td class="py-4 px-4 text-center">
                    <button class="delete-carteira-btn p-2 rounded-full hover:bg-red-500/20 text-gray-500 hover:text-red-400 transition-all" title="Remover" data-id="${item.id_parceiro}">
                        <span class="material-symbols-outlined text-lg">delete</span>
                    </button>
                </td>
            </tr>
        `;
    }

    /**
     * Configura os listeners da aba Carteira.
     */
    setupCarteiraEventListeners() {
        const container = document.getElementById('carteira-container');
        
        // Input de Arquivo
        const fileInput = document.getElementById('carteira-file-input');
        if (fileInput) {
            // Remove listener antigo para evitar duplicação (cloneNode truque) ou apenas reatribui
            const newFileInput = fileInput.cloneNode(true);
            fileInput.parentNode.replaceChild(newFileInput, fileInput);
            newFileInput.addEventListener('change', (e) => this.handleCarteiraFileUpload(e));
        }

        // Botão Manual
        const btnManual = document.getElementById('btn-open-carteira-manual');
        if (btnManual) {
            // Clone node para remover listeners antigos
            const newBtn = btnManual.cloneNode(true);
            btnManual.parentNode.replaceChild(newBtn, btnManual);
            newBtn.addEventListener('click', () => {
                document.getElementById('carteira-manual-modal').classList.add('active');
            });
        }
        // Botão Atualizar (Refresh)
        const btnRefresh = document.getElementById('btn-refresh-carteira');
        if (btnRefresh) {
            const newRefresh = btnRefresh.cloneNode(true);
            btnRefresh.parentNode.replaceChild(newRefresh, btnRefresh);
            newRefresh.addEventListener('click', async () => {
                this.sysledData = [];
                await this.loadCarteiraWithProgress();
            });
        }

        // Delegação de eventos para o botão Delete (elementos dinâmicos)
        if (container) {
            // Remove listeners antigos
            const headers = container.querySelectorAll('.sort-trigger');
            headers.forEach(th => {
                th.addEventListener('click', (e) => {
                    const col = e.currentTarget.dataset.col;
                    
                    // Alterna direção ou muda coluna
                    if (this.carteiraSortColumn === col) {
                        this.carteiraSortDirection = this.carteiraSortDirection === 'asc' ? 'desc' : 'asc';
                    } else {
                        this.carteiraSortColumn = col;
                        this.carteiraSortDirection = 'asc'; // Novo sort começa sempre ascendente (ou mude para desc se preferir)
                        
                        // Exceção: Para 'tempo_sem_envio' geralmente queremos ver os maiores (piores) primeiro
                        if (col === 'tempo_sem_envio') this.carteiraSortDirection = 'desc'; 
                    }
                    this.renderCarteiraTab(); // Re-renderiza com a nova ordem
                });
            });

            // Delete Buttons
            container.querySelectorAll('.delete-carteira-btn').forEach(btn => {
                btn.addEventListener('click', (e) => {
                    this.deleteCarteiraParceiro(e.currentTarget.dataset.id);
                });
            });
        }

        
    }

    injectCarteiraModal() {
        const modalHtml = `
            <div id="carteira-mapping-modal" class="modal fixed inset-0 z-50 items-center justify-center p-4 bg-black/50 backdrop-blur-sm">
                <div class="modal-content glass-card p-6 rounded-xl max-w-md w-full mx-4">
                    <h3 class="text-xl font-bold mb-4 text-white">Mapear Colunas - Carteira</h3>
                    <p id="file-name-carteira" class="text-sm text-gray-400 mb-4"></p>
                    <form id="carteira-mapping-form" class="space-y-4 mb-6"></form>
                    <div class="flex justify-end gap-2">
                        <button type="button" id="btn-cancel-carteira-map" class="px-4 py-2 rounded bg-gray-600 hover:bg-gray-500 text-white transition-colors">Cancelar</button>
                        <button type="button" id="btn-confirm-carteira-map" class="px-4 py-2 rounded bg-green-600 hover:bg-green-500 text-white transition-colors">Importar</button>
                    </div>
                </div>
            </div>
        `;
        document.body.insertAdjacentHTML('beforeend', modalHtml);
        
        document.getElementById('btn-cancel-carteira-map').addEventListener('click', () => document.getElementById('carteira-mapping-modal').classList.remove('active'));
        document.getElementById('btn-confirm-carteira-map').addEventListener('click', () => this.handleCarteiraMapping());
    }

    injectCarteiraManualModal() {
        const modalHtml = `
            <div id="carteira-manual-modal" class="modal fixed inset-0 z-50 items-center justify-center p-4 bg-black/50 backdrop-blur-sm">
                <div class="modal-content glass-card p-6 rounded-xl max-w-md w-full mx-4">
                    <h3 class="text-xl font-bold mb-4 text-white">Cadastrar Parceiro na Carteira</h3>
                    <form id="carteira-manual-form" class="space-y-4 mb-6">
                        <div>
                            <label for="carteira-manual-id" class="block text-sm font-medium text-gray-300 mb-1">ID Parceiro</label>
                            <input type="text" id="carteira-manual-id" class="glass-input w-full rounded-lg p-3" required placeholder="Digite o ID">
                        </div>
                        <div>
                            <label for="carteira-manual-nome" class="block text-sm font-medium text-gray-300 mb-1">Nome</label>
                            <input type="text" id="carteira-manual-nome" class="glass-input w-full rounded-lg p-3" required placeholder="Digite o Nome">
                        </div>
                    </form>
                    <div class="flex justify-end gap-2">
                        <button type="button" id="btn-cancel-carteira-manual" class="px-4 py-2 rounded bg-gray-600 hover:bg-gray-500 text-white transition-colors">Cancelar</button>
                        <button type="button" id="btn-confirm-carteira-manual" class="px-4 py-2 rounded bg-green-600 hover:bg-green-500 text-white transition-colors">Salvar</button>
                    </div>
                </div>
            </div>
        `;
        document.body.insertAdjacentHTML('beforeend', modalHtml);
        
        document.getElementById('btn-cancel-carteira-manual').addEventListener('click', () => {
            document.getElementById('carteira-manual-modal').classList.remove('active');
            document.getElementById('carteira-manual-form').reset();
        });
        
        document.getElementById('btn-confirm-carteira-manual').addEventListener('click', () => this.handleCarteiraManualSubmit());
    }

    handleCarteiraFileUpload(event) {
        const file = event.target.files[0];
        if (!file) return;
        
        const fileNameDisplay = document.getElementById('file-name-carteira');
        if(fileNameDisplay) fileNameDisplay.textContent = `Arquivo: ${file.name}`;

        const reader = new FileReader();
        reader.onload = (e) => {
            const data = new Uint8Array(e.target.result);
            const workbook = XLSX.read(data, { type: 'array' });
            this.tempCarteiraData = XLSX.utils.sheet_to_json(workbook.Sheets[workbook.SheetNames[0]], { raw: false });
            const headers = this.tempCarteiraData.length > 0 ? Object.keys(this.tempCarteiraData[0]) : [];
            
            const form = document.getElementById('carteira-mapping-form');
            const modal = document.getElementById('carteira-mapping-modal');
            if (!form || !modal) return;

            form.innerHTML = '';
            const fields = { id_parceiro: 'ID Parceiro', nome: 'Nome' };
            
            for (const key in fields) {
                const options = headers.map(h => `<option value="${h}" class="bg-background-dark">${h}</option>`).join('');
                form.innerHTML += `
                    <div class="grid grid-cols-2 gap-4 items-center">
                        <label class="font-medium text-gray-300">${fields[key]}</label>
                        <select name="${key}" class="glass-input w-full p-2 rounded-lg bg-background-dark/50 border border-white/10 text-white">
                            <option value="" class="bg-background-dark">Selecione...</option>
                            ${options}
                        </select>
                    </div>`;
            }
            
            modal.classList.add('active');
        };
        reader.readAsArrayBuffer(file);
        event.target.value = '';
    }

    async handleCarteiraMapping() {
        const mapping = {};
        document.getElementById('carteira-mapping-form').querySelectorAll('select').forEach(s => { mapping[s.name] = s.value; });

        if (!mapping.id_parceiro || !mapping.nome) {
            alert("Todos os campos são obrigatórios.");
            return;
        }

        const dataToInsert = this.tempCarteiraData.map(row => ({
            id_parceiro: String(row[mapping.id_parceiro] || ''),
            nome: row[mapping.nome]
        })).filter(item => item.id_parceiro && item.nome);

        if (dataToInsert.length === 0) {
            alert("Nenhum dado válido encontrado.");
            return;
        }

        // Verifica quais IDs já existem para separar Insert de Update (workaround para falta de constraint unique)
        const idsToCheck = dataToInsert.map(d => d.id_parceiro);
        const { data: existingData, error: fetchError } = await supabase
            .from('carteira')
            .select('id_parceiro')
            .in('id_parceiro', idsToCheck);

        if (fetchError) {
            alert("Erro ao verificar existentes: " + fetchError.message);
            return;
        }

        const existingIds = new Set(existingData.map(d => d.id_parceiro));
        const toInsert = dataToInsert.filter(d => !existingIds.has(d.id_parceiro));
        const toUpdate = dataToInsert.filter(d => existingIds.has(d.id_parceiro));

        let errorMsg = '';

        if (toInsert.length > 0) {
            const { error } = await supabase.from('carteira').insert(toInsert);
            if (error) errorMsg += "Erro ao inserir: " + error.message + "\n";
        }

        if (toUpdate.length > 0) {
            const promises = toUpdate.map(item => supabase.from('carteira').update({ nome: item.nome }).eq('id_parceiro', item.id_parceiro));
            const results = await Promise.all(promises);
            const errors = results.filter(r => r.error).map(r => r.error.message);
            if (errors.length > 0) errorMsg += "Erros ao atualizar: " + errors.join(', ') + "\n";
        }

        if (errorMsg) {
            alert("Ocorreram erros durante a importação:\n" + errorMsg);
        } else {
            alert(`${dataToInsert.length} registros importados/atualizados com sucesso!`);
            await this.logAction(`Importou ${dataToInsert.length} registros para a Carteira.`);
            document.getElementById('carteira-mapping-modal').classList.remove('active');
            await this.loadData();
            this.renderAll();
        }
    }

    async handleCarteiraManualSubmit() {
        const id = document.getElementById('carteira-manual-id').value.trim();
        const nome = document.getElementById('carteira-manual-nome').value.trim();

        if (!id || !nome) {
            alert("ID e Nome são obrigatórios.");
            return;
        }

        // Tenta garantir que temos dados da Sysled para mostrar o feedback no alert
        if (this.sysledData.length === 0) {
            try {
                // Tenta buscar silenciosamente se não tiver dados
                const response = await fetch(this.sysledApiUrl, { headers: { 'Authorization': this.sysledAuthToken } });
                if (response.ok) this.sysledData = await response.json();
            } catch (e) {
                console.warn("Não foi possível buscar dados da Sysled para preview.", e);
            }
        }

        // Calcula KPIs apenas para mostrar no alerta (NÃO SALVA NO DB)
        const kpis = this.calculatePartnerKPIs(id, this.sysledData);

        // Verifica se já existe
        const { data: existing, error: checkError } = await supabase.from('carteira').select('id_parceiro').eq('id_parceiro', id).maybeSingle();
        
        if (checkError) {
            alert("Erro ao verificar parceiro: " + checkError.message);
            return;
        }

        let actionError;
        // Salva APENAS id e nome no Supabase
        const payload = { id_parceiro: id, nome: nome };

        if (existing) {
            const { error } = await supabase.from('carteira').update(payload).eq('id_parceiro', id);
            actionError = error;
        } else {
            const { error } = await supabase.from('carteira').insert([payload]);
            actionError = error;
        }

        if (actionError) {
            alert("Erro ao cadastrar parceiro: " + actionError.message);
        } else {
            // Feedback rico para o usuário
            alert(`Parceiro cadastrado com sucesso!\n\nDados da Sysled:\n- Projetos Fechados: ${kpis.projeto_fechado}\n- Projetos Enviados: ${kpis.projeto_enviado}\n- Saúde: ${kpis.saude_carteira}`);
            
            await this.logAction(`Cadastrou manualmente parceiro na Carteira: ${nome} (ID: ${id})`);
            document.getElementById('carteira-manual-modal').classList.remove('active');
            document.getElementById('carteira-manual-form').reset();
            await this.loadData();
            this.renderAll();
        }
    }

    async deleteCarteiraParceiro(id) {
        const parceiro = this.carteira.find(c => String(c.id_parceiro) === String(id));
        if (!parceiro) return;

        if (confirm(`Tem certeza que deseja remover ${parceiro.nome} (ID: ${id}) da Carteira?`)) {
            const { error } = await supabase.from('carteira').delete().eq('id_parceiro', id);

            if (error) {
                alert("Erro ao remover parceiro: " + error.message);
            } else {
                this.carteira = this.carteira.filter(c => String(c.id_parceiro) !== String(id));
                await this.logAction(`Removeu parceiro da Carteira: ${parceiro.nome} (ID: ${id})`);
                this.renderCarteiraTab();
            }
        }
    }
}

export default RelacionamentoApp;