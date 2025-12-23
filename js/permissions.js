import { supabase } from './utils.js';

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
            const { data: { session } } = await supabase.auth.getSession();
            if (!session) {
                console.warn('Nenhuma sessão ativa');
                return false;
            }

            const userId = session.user.id;

            // Busca o role do usuário
            const { data: userRoleData, error: roleError } = await supabase
                .from('user_roles')
                .select('role')
                .eq('user_id', userId)
                .single();

            if (roleError) {
                console.error('Erro ao buscar role do usuário:', roleError);
                // Se não tem role definido, considera como viewer por padrão
                this.userRole = 'viewer';
            } else {
                this.userRole = userRoleData.role;
            }

            // Busca as permissões do role
            const { data: permissions, error: permError } = await supabase
                .from('role_permissions')
                .select('permission')
                .eq('role', this.userRole);

            if (permError) {
                console.error('Erro ao buscar permissões:', permError);
                return false;
            }

            // Armazena as permissões em um Set para busca rápida
            this.userPermissions = new Set(permissions.map(p => p.permission));
            this.isLoaded = true;

            console.log(`Permissões carregadas para role: ${this.userRole}`, [...this.userPermissions]);
            return true;

        } catch (error) {
            console.error('Erro ao carregar permissões:', error);
            return false;
        }
    }

    /**
     * Verifica se o usuário tem uma permissão específica
     */
    hasPermission(permission) {
        if (!this.isLoaded) {
            console.warn('Permissões ainda não foram carregadas');
            return false;
        }
        return this.userPermissions.has(permission);
    }

    /**
     * Verifica se o usuário tem QUALQUER uma das permissões listadas
     */
    hasAnyPermission(permissions) {
        return permissions.some(perm => this.hasPermission(perm));
    }

    /**
     * Verifica se o usuário tem TODAS as permissões listadas
     */
    hasAllPermissions(permissions) {
        return permissions.every(perm => this.hasPermission(perm));
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
        return this.userRole === 'admin';
    }

    /**
     * Verifica se é manager
     */
    isManager() {
        return this.userRole === 'manager';
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
    'view_import_vendas': ['import_data'],
    'view_consulta_sysled': ['view_relatorios'],
    'view_inclusao_manual': ['manage_comissoes', 'view_comissoes'],
    'view_arquitetos': ['view_arquitetos'],
    'view_pontuacao': ['view_relatorios'],
    'view_comprovantes': ['view_pagamentos'],
    'view_resgates': ['view_pagamentos'],
    'view_arquivos': ['view_relatorios'],
    'view_resultados': ['view_relatorios'],
    'view_eventos': ['view_logs'],

    // Ações específicas
    'import_vendas': ['import_data'],
    'export_arquitetos': ['export_data'],
    'add_arquiteto': ['edit_arquitetos'],
    'edit_arquiteto': ['edit_arquitetos'],
    'delete_arquiteto': ['delete_arquitetos'],
    'manage_comissoes': ['manage_comissoes'],
    'approve_comissoes': ['approve_pagamentos'],
    'manage_pagamentos': ['manage_pagamentos'],
    'gerar_pagamentos': ['manage_pagamentos'],
    'manage_pontos': ['manage_comissoes'],
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

    // Controle de visibilidade de abas do menu
    const menuItems = {
        'relatorio-rt': canPerformAction('view_import_vendas'),
        'consulta-sysled': canPerformAction('view_consulta_sysled'),
        'inclusao-manual': canPerformAction('view_inclusao_manual'),
        'arquitetos': canPerformAction('view_arquitetos'),
        'pontuacao': canPerformAction('view_pontuacao'),
        'comprovantes': canPerformAction('view_comprovantes'),
        'resgates': canPerformAction('view_resgates'),
        'arquivos-importados': canPerformAction('view_arquivos'),
        'resultados': canPerformAction('view_resultados'),
        'eventos': canPerformAction('view_eventos'),
    };

    // Esconde/mostra itens do menu
    Object.entries(menuItems).forEach(([tabName, hasPermission]) => {
        const menuLink = document.querySelector(`.menu-link[data-tab="${tabName}"]`);
        if (menuLink) {
            menuLink.style.display = hasPermission ? '' : 'none';
        }
    });

    // Controle de botões específicos
    const buttonPermissions = {
        'export-csv-btn': canPerformAction('export_arquitetos'),
        'delete-all-arquitetos-btn': permissionsManager.isAdmin(),
        'gerar-pagamentos-rt-btn': canPerformAction('gerar_pagamentos'),
        'aprovar-inclusao-manual-btn': canPerformAction('approve_comissoes'),
    };

    Object.entries(buttonPermissions).forEach(([buttonId, hasPermission]) => {
        const button = document.getElementById(buttonId);
        if (button) {
            if (!hasPermission) {
                button.style.display = 'none';
            }
        }
    });

    // Exibe badge com o role do usuário
    displayUserRole();
}

/**
 * Exibe o role do usuário na interface
 */
function displayUserRole() {
    const roleLabels = {
        'admin': { text: 'Administrador', color: 'bg-red-500/80' },
        'manager': { text: 'Gestor', color: 'bg-blue-500/80' },
        'operator': { text: 'Operador', color: 'bg-green-500/80' },
        'viewer': { text: 'Visualizador', color: 'bg-gray-500/80' }
    };

    const role = permissionsManager.getUserRole();
    const roleInfo = roleLabels[role] || { text: 'Usuário', color: 'bg-gray-500/80' };

    // Adiciona badge no header
    const header = document.querySelector('header .flex.items-center.gap-4');
    if (header && !document.getElementById('user-role-badge')) {
        const badge = document.createElement('span');
        badge.id = 'user-role-badge';
        badge.className = `px-3 py-1 rounded-full text-xs font-semibold text-white ${roleInfo.color}`;
        badge.textContent = roleInfo.text;
        header.insertBefore(badge, header.firstChild);
    }
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
            alert('Você não tem permissão para realizar esta ação.');
        }
        return false;
    }
}