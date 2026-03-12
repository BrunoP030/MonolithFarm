# Checklist de Primeiro Uso na Faculdade

Este checklist foi preparado para reduzir erros de ambiente ao executar o projeto em outra máquina.

## Antes de começar

- confirmar se o `Python 3.11` está instalado:

```powershell
python --version
```

- confirmar se o projeto foi clonado ou baixado corretamente;
- confirmar se o pacote `FarmLab` está disponível na máquina;
- identificar o caminho exato onde o diretório `FarmLab` foi salvo.

## Checklist rápido

1. abrir o terminal na pasta do projeto;
2. verificar se o diretório de dados existe;
3. rodar o script de inicialização com atualização do banco;
4. abrir o painel no navegador;
5. manter o terminal aberto enquanto o painel estiver em uso.

## Conferência do diretório de dados

Se os dados estiverem no caminho padrão:

```text
C:\Users\Morgado\Downloads\FarmLab
```

o comando pode ser executado diretamente.

Se os dados estiverem em outro local, usar esse caminho explicitamente:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\start_dashboard.ps1 -Refresh -DataDir "D:\Caminho\FarmLab"
```

## Comando recomendado

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\start_dashboard.ps1 -Refresh
```

## Se a porta 8501 estiver ocupada

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\start_dashboard.ps1 -Refresh -Port 8502
```

Depois acessar:

```text
http://127.0.0.1:8502
```

## Se algo der errado

- se o terminal disser que o diretório de dados não foi encontrado, conferir o caminho do `FarmLab`;
- se a porta estiver ocupada, trocar a porta com `-Port 8502`;
- se o `Python` não for reconhecido, instalar ou corrigir o PATH do sistema;
- se o painel não abrir, verificar a URL exibida no terminal.

## Verificação final

Ao final da inicialização, o cenário esperado é:

- ambiente virtual criado;
- dependências instaladas;
- banco local atualizado;
- painel aberto no navegador;
- projeto pronto para apresentação ou análise.
