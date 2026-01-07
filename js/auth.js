import { supabase } from './utils.js';

document.addEventListener('DOMContentLoaded', () => {
    const loginForm = document.getElementById('login-form');
    const logoutButton = document.getElementById('logout-button');
    const authError = document.getElementById('auth-error');

    // Lógica do Formulário de Login
    if (loginForm) {
        loginForm.addEventListener('submit', async (e) => {
            e.preventDefault();
            const email = document.getElementById('email').value;
            const password = document.getElementById('password').value;
            
            if (authError) authError.textContent = 'Autenticando...';

            const { data, error } = await supabase.auth.signInWithPassword({ email, password });
            
            if (error) {
                if (authError) authError.textContent = 'Erro: ' + error.message;
            } else {
                // SUCESSO: Recarrega a página. 
                // O app.js vai rodar o init(), ver o token e abrir o dashboard.
                window.location.reload();
            }
        });
    }

    // Lógica do Botão Sair
    if (logoutButton) {
        logoutButton.addEventListener('click', async (e) => {
            e.preventDefault();
            
            // Tenta fazer o signout
            const { error } = await supabase.auth.signOut();
            
            // INDEPENDENTE se deu erro (ex: session missing) ou sucesso,
            // nós limpamos o estado local e recarregamos.
            if (error) {
                console.warn('Aviso ao sair:', error.message);
            }
            
            // Limpa forçadamente qualquer resquício local do Supabase (opcional, mas recomendado)
            localStorage.removeItem('sb-api-auth-token'); // Ajuste conforme a chave do seu projeto se souber, senão o reload resolve
            
            // Recarrega a página para voltar ao Login
            window.location.reload();
        });
    }
});