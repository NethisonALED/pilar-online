import { supabase, showToast } from "./utils.js";

/**
 * Classe para gerenciar permissões do usuário
 */
class PermissionsManager {
  constructor() {
    this.userPermissions = new Set();
    this.userRole = null;
    this.isLoaded = false;
  }

  
  /**
   * Carrega as permissões do usuário atual
   */
async loadUserPermissions() {
    try {
      // 1. Pega o usuário logado da sessão (Isso vem do auth.users, que funciona)
      const { data: { session } } = await supabase.auth.getSession();
      if (!session) {
        console.warn("Nenhuma sessão ativa");
        return false;
      }

      const userEmail = session.user.email;

      // 2. Busca na tabela pública 'perfis'
      // Usamos .ilike para ignorar maiúsculas/minúsculas no email
      // Usamos .maybeSingle() para não dar erro vermelho no console se não achar
      const { data: userProfile, error: profileError } = await supabase
        .from("perfis") 
        .select("nivel_acesso") 
        .ilike("email", userEmail) 
        .maybeSingle();

      if (profileError) {
        console.error("Erro de banco ao buscar perfil:", profileError);
        this.userRole = "visualizador"; 
      } else if (!userProfile) {
        console.warn("Perfil não encontrado na tabela 'perfis' para este email.");
        this.userRole = "visualizador";
      } else {
        this.userRole = userProfile.nivel_acesso; 
      }


      // Removemos a busca por role_permissions que não existe
      this.isLoaded = true;
      return true;

    } catch (error) {
      console.error("Erro crítico em loadUserPermissions:", error);
      return false;
    }
  }

  /**
   * Verifica se o usuário tem uma permissão específica
   */
  hasPermission(permission) {
    if (!this.isLoaded) {
      console.warn("Permissões ainda não foram carregadas");
      return false;
    }
    return this.userPermissions.has(permission);
  }

  /**
   * Verifica se o usuário tem QUALQUER uma das permissões listadas
   */
  hasAnyPermission(permissions) {
    return permissions.some((perm) => this.hasPermission(perm));
  }

  /**
   * Verifica se o usuário tem TODAS as permissões listadas
   */
  hasAllPermissions(permissions) {
    return permissions.every((perm) => this.hasPermission(perm));
  }

  /**
   * Retorna o role do usuário
   */
  getUserRole() {
    return this.userRole;
  }

  /**
   * Verifica se é admin
   */
  isAdmin() {
    return this.userRole === "admin";
  }

  /**
   * Verifica se é manager
   */
  isManager() {
    return this.userRole === "manager" || this.userRole === "editor";
  }

  /**
   * Limpa as permissões (útil no logout)
   */
  clear() {
    this.userPermissions.clear();
    this.userRole = null;
    this.isLoaded = false;
  }
}


// Exporta uma instância única (singleton)
export const permissionsManager = new PermissionsManager();

/**
 * Mapeamento de funcionalidades para permissões necessárias
 */
export const PERMISSIONS_MAP = {
  // Navegação/Visualização
  view_import_vendas: ["import_data"],
  view_consulta_sysled: ["view_relatorios"],
  view_inclusao_manual: ["manage_comissoes", "view_comissoes"],
  view_arquitetos: ["view_arquitetos"],
  view_pontuacao: ["view_relatorios"],
  view_comprovantes: ["view_pagamentos"],
  view_resgates: ["view_pagamentos"],
  view_arquivos: ["view_relatorios"],
  view_resultados: ["view_relatorios"],
  view_eventos: ["view_logs"],

  // Ações específicas
  import_vendas: ["import_data"],
  export_arquitetos: ["export_data"],
  add_arquiteto: ["edit_arquitetos"],
  edit_arquiteto: ["edit_arquitetos"],
  delete_arquiteto: ["delete_arquitetos"],
  manage_comissoes: ["manage_comissoes"],
  approve_comissoes: ["approve_pagamentos"],
  manage_pagamentos: ["manage_pagamentos"],
  gerar_pagamentos: ["manage_pagamentos"],
  manage_pontos: ["manage_comissoes"],
};

/**
 * Função auxiliar para verificar permissão de uma ação
 */
export function canPerformAction(actionKey) {
  const requiredPermissions = PERMISSIONS_MAP[actionKey];
  if (!requiredPermissions) {
    console.warn(`Ação não mapeada: ${actionKey}`);
    return false;
  }
  return permissionsManager.hasAnyPermission(requiredPermissions);
}

/**
 * Aplica controles de UI baseados nas permissões do usuário
 */
export function applyUIPermissions() {
    if (!permissionsManager.isLoaded) {
        console.warn('Permissões não carregadas ainda');
        return;
    }

    const role = permissionsManager.getUserRole();
    console.log(`Aplicando regras de UI para o cargo: ${role}`);

    // --- 1. CONTROLE DO MENU LATERAL ---
    
    // Lista de abas que o VISUALIZADOR (Vendedor) pode ver
    // 'carteira' = Aba Carteira
    // 'crm-opportunities' = Aba Oportunidades CRM
    // 'comprovantes' = Geralmente vendedor precisa ver seus comprovantes (opcional, adicione se quiser)
    const visualizadorAllowedTabs = ['carteira', 'crm-opportunities'];

    const menuLinks = document.querySelectorAll('.menu-link');

    menuLinks.forEach(link => {
        const tabName = link.dataset.tab;

        // Regra: Editor/Admin/Gestor vê TUDO. Visualizador vê FILTRADO.
        if (role === 'admin' || role === 'editor' || role === 'gestor') {
            link.style.display = 'flex'; // Mostra tudo
        } else {
            // É visualizador (ou qualquer outro)
            if (visualizadorAllowedTabs.includes(tabName)) {
                link.style.display = 'flex';
            } else {
                link.style.display = 'none'; // Esconde o resto
            }
        }
    });


    // --- 2. CONTROLE DE BOTÕES ESPECÍFICOS ---
    
    // Se for visualizador, escondemos botões de "Deletar", "Editar", "Exportar CSV" etc.
    const isRestricted = (role !== 'admin' && role !== 'editor' && role !== 'gestor');

    const restrictedButtons = [
        'export-csv-btn',           // Exportar Excel
        'delete-all-arquitetos-btn', // Deletar Tudo
        'gerar-pagamentos-rt-btn',   // Gerar Pagamento em Lote
        'aprovar-inclusao-manual-btn', // Aprovar Comissão
        'add-arquiteto-form',        // Formulário de adicionar
        'btn-open-carteira-manual'   // Adicionar parceiro manual na carteira
    ];

    restrictedButtons.forEach(btnId => {
        const btn = document.getElementById(btnId);
        if (btn) {
            btn.style.display = isRestricted ? 'none' : '';
        }
    });
    
    // Remove botões de delete/edit das tabelas via CSS se for restrito
    if (isRestricted) {
        // Exemplo: Esconder botões de ação na tabela de arquitetos via classe global
        document.body.classList.add('user-restricted');
    } else {
        document.body.classList.remove('user-restricted');
    }

    // Exibe badge com o role do usuário
    displayUserRole();
}

/**
 * Exibe o role do usuário na interface
 */
function displayUserRole() {
    // Badge desabilitado no header a pedido do usuário.
}

/**
 * Middleware para verificar permissão antes de executar uma ação
 */
export function requirePermission(actionKey, callback, deniedCallback = null) {
  if (canPerformAction(actionKey)) {
    return callback();
  } else {
    console.warn(`Permissão negada para: ${actionKey}`);
    if (deniedCallback) {
      deniedCallback();
    } else {
      showToast("Você não tem permissão para realizar esta ação.", "error");
    }
    return false;
  }
}
