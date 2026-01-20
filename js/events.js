/**
 * Inicializa todos os event listeners da aplicação.
 * Utiliza delegação de eventos no container principal para otimizar o desempenho.
 * @param {RelacionamentoApp} app - A instância principal da classe da aplicação.
 */
export function initializeEventListeners(app) {
  const mainContainer = document.getElementById("app-container");
  if (!mainContainer) {
    console.error("Container principal #app-container não encontrado.");
    return;
  } // --- NAVEGAÇÃO POR ABAS ---

  const menuLinks = document.querySelectorAll(".menu-link");
  const tabViews = document.querySelectorAll(".tab-view");

  menuLinks.forEach((link) => {
    link.addEventListener("click", (e) => {
      e.preventDefault();
      if (link.id === "logout-button") return;

      const targetTab = link.dataset.tab;

      // 1. LIMPEZA COMPLETA (Remove active E as classes visuais cinzas/brancas)
      menuLinks.forEach((l) => {
        l.classList.remove("active", "bg-white/10", "text-white");
      });

      // 2. ATIVAÇÃO COMPLETA (Adiciona active E as classes visuais)
      link.classList.add("active", "bg-white/10", "text-white");

      // 3. Troca de Views (Lógica de CSS)
      tabViews.forEach((view) => {
        // Verifica se é a view alvo
        const isActive =
          view.id === `${targetTab}-view` ||
          view.id === `${targetTab}-container`; // Ajuste para garantir compatibilidade com seus IDs

        // Toggle para garantir
        view.classList.toggle("active", isActive);

        // Lógica explícita de hidden para garantir que suma
        if (isActive) {
          view.classList.remove("hidden");
        } else {
          view.classList.add("hidden");
        }
      });

      // 4. Renderização Específica
      if (targetTab === "carteira") {
        app.renderCarteiraTab();
      }

      // ADICIONE ISTO:
      if (targetTab === "crm-opportunities") {
          app.renderCrmTab();
      }

      if (targetTab === "consulta-sysled") {
          // Verifica se já existem dados na memória
          if (app.sysledData && app.sysledData.length > 0) {
              // Se JÁ tem dados, apenas redesenha a tabela (instantâneo)
              console.log("Dados em cache encontrados. Renderizando...");
              app.renderSysledTable();
          } else {
              // Se NÃO tem dados (primeira vez ou F5), busca na API
              app.fetchSysledData(); 
          }
      }

      // 5. Atualiza URL
      const url = new URL(window.location);
      url.searchParams.set("tab", targetTab);
      window.history.pushState({}, "", url);
    });
  }); // --- DELEGAÇÃO DE EVENTOS DE CLIQUE ---

  mainContainer.addEventListener("click", (e) => {
    const target = e.target; // Ações gerais

    if (target.closest("#sidebar-toggle-btn"))
      document.getElementById("app-sidebar").classList.toggle("collapsed");
    if (target.closest("#calculate-results-btn")) app.renderResultados();
    if (target.closest("#profile-picture-container"))
      document.getElementById("profile-picture-input").click(); // Ações da aba Arquitetos e Modais relacionados

    if (target.closest(".edit-btn")) app.handleArquitetosTableClick(e);
    if (target.closest(".delete-btn")) app.handleArquitetosTableClick(e);
    if (target.closest(".add-value-btn")) app.handleArquitetosTableClick(e);
    if (target.closest(".id-link")) {
      e.preventDefault();
      app.handleArquitetosTableClick(e);
    }
    if (target.closest(".sortable-header")) app.handleSort(e);
    if (target.closest("#export-csv-btn")) app.exportArquitetosCSV();
    if (target.closest("#delete-all-arquitetos-btn")) app.deleteAllArquitetos();
    if (target.closest("#gerar-pagamentos-rt-btn"))
      app.handleGerarPagamentosClick();
    if (target.closest("#close-edit-modal-x-btn")) app.closeEditModal();
    if (target.closest("#gerar-pagamento-ficha-btn"))
      app.handleGerarPagamentoFicha();
    if (target.closest("#gerar-resgate-ficha-btn"))
      app.handleGerarResgateFicha(); // NOVO
    if (target.closest("#consultar-vendas-btn"))
      app.handleConsultarVendasClick(e);
    if (target.closest("#cancel-add-value-btn")) app.closeAddValueModal();
    if (target.closest("#cancel-arquiteto-mapping-btn"))
      app.closeArquitetoMappingModal();
    if (target.closest("#confirm-arquiteto-mapping-btn"))
      app.handleArquitetoMapping(); // Ações de importação de vendas (RT)
    if (target.closest("#cancel-rt-mapping-btn")) app.closeRtMappingModal();
    if (target.closest("#confirm-rt-mapping-btn")) app.handleRtMapping(); // Ações das abas Comprovantes/Resgates e Modais relacionados

    if (target.closest(".view-comprovante-btn")) app.handlePagamentosClick(e);
    if (target.closest(".delete-pagamentos-btn")) app.handlePagamentosClick(e);
    if (target.closest(".download-xlsx-btn")) app.handlePagamentosClick(e);
    if (target.closest(".gerar-relatorio-btn")) app.handlePagamentosClick(e);
    if (target.closest(".edit-rt-btn")) app.handlePagamentosClick(e);
    if (target.closest("#close-comprovante-modal-btn"))
      app.closeComprovanteModal();
    if (target.closest("#cancel-edit-rt-btn")) app.closeEditRtModal();
    if (target.closest("#cancel-gerar-pagamentos-btn"))
      document
        .getElementById("gerar-pagamentos-modal")
        .classList.remove("flex");
    if (target.closest("#confirmar-geracao-comprovantes-btn"))
      app.confirmarGeracaoComprovantes(); // Ações da aba Arquivos Importados

    if (target.closest(".download-arquivo-btn"))
      app.handleArquivosImportadosClick(e); // Ações da aba Consulta Sysled e Modais relacionados

    if (target.closest("#sysled-refresh-btn")) app.fetchSysledData();
    if (target.closest("#sysled-filter-btn")) app.renderSysledTable();
    if (target.closest("#sysled-clear-filter-btn")) app.clearSysledFilters();
    if (target.closest("#copy-to-rt-btn")) app.handleCopyToRTClick();
    if (target.closest(".view-sale-details-btn")) {
      e.preventDefault();
      app.handleSalesHistoryTableClick(e);
    }
    if (target.closest("#close-sales-history-btn"))
      app.closeSalesHistoryModal();
    if (target.closest("#import-single-sale-btn"))
      app.handleImportSingleSale(e);
    if (target.closest("#close-sale-details-btn")) app.closeSaleDetailsModal();
    // Ações da aba Inclusão Manual e Modais relacionados
    if (target.closest(".view-comissao-details-btn")) {
      e.preventDefault();
      app.handleHistoricoManualClick(e);
    }
    if (target.closest("#close-comissao-manual-details-btn"))
      app.closeComissaoManualDetailsModal();
    if (target.closest("#aprovar-inclusao-manual-btn"))
      app.handleAprovarInclusaoManual(e);

    // Ações do Modal de Novo Arquiteto
    if (target.closest("#cancel-novo-arquiteto-btn")) app.cancelNovoArquiteto();

    // Ações da aba Consulta Sysled
    if (target.closest("#sysled-refresh-btn")) app.fetchSysledData();
    if (target.closest("#sysled-filter-btn")) app.renderSysledTable();
    if (target.closest("#sysled-clear-filter-btn")) {
      // Resetar página ao limpar filtro
      app.sysledPage = 1;
      app.clearSysledFilters();
    }

    if (target.closest("#sysled-clear-search-btn")) {
      const input = document.getElementById("sysled-filter-search");
      if (input) {
        input.value = ""; // Limpa o texto
        input.focus(); // Devolve o foco para digitar de novo
        app.sysledPage = 1;
        app.renderSysledTable(); // Atualiza a tabela (vai esconder o X automaticamente no render)
      }
    }

    if (target.closest("#sysled-clear-search-btn")) {
        // ... (código existente)
    }

    // --- ADICIONE ISTO AQUI ---
    const deleteSysledBtn = target.closest(".delete-sysled-item-btn");
    if (deleteSysledBtn) {
        app.handleDeleteSysled(deleteSysledBtn.dataset.id);
    }

    // --- NOVO: Paginação Sysled ---
    const pageBtn = target.closest(".sysled-page-btn");
    if (pageBtn) {
      app.changeSysledPage(pageBtn.dataset.action);
    }
  }); // --- DELEGAÇÃO DE EVENTOS DE SUBMISSÃO (FORMULÁRIOS) ---

  mainContainer.addEventListener("submit", (e) => {
    e.preventDefault(); // Impede o comportamento padrão de todos os formulários
    switch (e.target.id) {
      case "add-comissao-manual-form":
        app.handleAddComissaoManual(e);
        break;
      case "add-arquiteto-form":
        app.handleAddArquiteto(e);
        break;
      case "edit-arquiteto-form":
        app.handleEditArquiteto(e);
        break;
      case "add-value-form":
        app.handleAddValue(e);
        break;

      case "edit-rt-form":
        app.handleUpdateRtValue(e);
        break;
      case "change-password-form":
        app.handleChangePassword(e);
        break;
      case "novo-arquiteto-form":
        app.handleNovoArquitetoSubmit(e);
        break;
    }
  }); 
  
  // --- DELEGAÇÃO DE EVENTOS DE INPUT E CHANGE ---
  mainContainer.addEventListener("input", (e) => {
        const target = e.target;
        const id = target.id;

        switch (id) {
            // Aba Arquitetos (Se ainda existir no seu HTML)
            case 'arquiteto-search-input':
                app.renderArquitetosTable();
                break;

            // Aba Sysled (Lógica Unificada: Botão X + Render)
            case 'sysled-filter-search':
                // 1. Controle visual do botão X
                const clearBtn = document.getElementById('sysled-clear-search-btn');
                if (clearBtn) {
                    if (target.value.trim().length > 0) clearBtn.classList.remove('hidden');
                    else clearBtn.classList.add('hidden');
                }
                
                // 2. Filtro e Render
                app.sysledPage = 1; // Sempre volta pra pág 1 ao digitar
                app.renderSysledTable(); 
                break;

            // Aba Pagamentos
            case "pagamento-search-input":
                app.renderPagamentos(target.value.trim());
                break;

            // Aba Resgates
            case "resgate-search-input":
                app.renderResgates(target.value.trim());
                break;
        }
    });

  mainContainer.addEventListener("change", (e) => {
    switch (e.target.id) {

      case "arquiteto-file-input":
        app.handleArquitetoFileUpload(e);
        break;
      case "rt-percentual":
        app.calculateRT();
        break;
      case "profile-picture-input":
        app.handleProfilePictureUpload(e);
        break;
      case "min-payment-value":
        app.minPaymentValue = parseFloat(e.target.value) || 0;
        app.renderArquitetosTable();
        break;
    }
    if (e.target.matches(".pagamento-status, .comprovante-input")) {
      app.handlePagamentosChange(e);
    }
  });

  console.log("Event listeners configurados.");
}
