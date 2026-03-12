# MonolithFarm

Plataforma analítica para comparação entre áreas de milho com manejo convencional e áreas acompanhadas por tecnologias 4.0, com foco em NDVI, produtividade, clima, pragas e contexto operacional.

## Resumo

O presente projeto propõe a construção de um MVP analítico capaz de integrar diferentes fontes de dados agrícolas para apoiar a interpretação do desempenho de duas áreas de cultivo de milho. A solução reúne informações de sensoriamento remoto, monitoramento de pragas, clima, análise de solo e operações de campo, com o objetivo de oferecer uma leitura técnica mais clara sobre diferenças de vigor vegetativo e produtividade.

Em contexto acadêmico, o sistema foi concebido como uma base aplicada para investigação, comparação e argumentação. Sua principal finalidade é apoiar a discussão sobre o comportamento de áreas convencionais e áreas com tecnologia 4.0, especialmente a partir do uso do índice NDVI como eixo central da análise.

## Objetivo Geral

Desenvolver uma plataforma visual e analítica capaz de consolidar dados agrícolas heterogêneos e sustentar, de forma técnica, a comparação entre áreas de milho com manejo convencional e áreas acompanhadas por tecnologias 4.0.

## Objetivos Específicos

- organizar, tratar e persistir os dados disponibilizados no pacote `FarmLab`;
- relacionar informações de NDVI, produtividade, clima, solo, armadilhas e operações de campo;
- permitir a visualização comparativa entre recortes monitorados;
- apoiar a formulação de hipóteses sobre variações de desenvolvimento vegetativo e desempenho produtivo;
- preparar uma base consistente para discussão de eficiência operacional e, futuramente, eficiência econômica.

## Escopo Atual da Solução

No estágio atual, o projeto:

- lê automaticamente o conjunto de dados `FarmLab`;
- consolida informações de NDVI, solo, clima, armadilhas e camadas operacionais;
- materializa os dados tratados em um banco local `DuckDB`;
- disponibiliza um painel visual em `Streamlit`;
- sugere um vínculo inicial entre recortes de NDVI e áreas monitoradas;
- aceita arquivos auxiliares para mapeamento manual das áreas e custos por hectare.

## Estrutura da Solução

O sistema está organizado em quatro camadas principais:

- ingestão: descoberta e leitura automática dos arquivos brutos;
- tratamento: normalização e preparação dos dados;
- persistência: materialização em banco local `DuckDB`;
- visualização: painel analítico construído em `Streamlit`.

## Tecnologias Utilizadas

- `Python`
- `Streamlit`
- `DuckDB`
- `Pandas`
- `Plotly`
- `Shapely`
- `PyProj`

## Dados Esperados

Por padrão, o projeto procura os dados no diretório:

```text
C:\Users\Morgado\Downloads\FarmLab
```

Caso o pacote esteja em outro local, o caminho pode ser informado manualmente no script de inicialização ou na própria interface.

## Execução

O guia detalhado de execução está disponível em:

- [COMO_EXECUTAR.md](docs/COMO_EXECUTAR.md)
- [PRIMEIRO_USO_FACULDADE.md](docs/PRIMEIRO_USO_FACULDADE.md)

Para uma execução rápida após baixar o repositório:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\start_dashboard.ps1 -Refresh
```

Depois disso, o painel poderá ser acessado em:

```text
http://127.0.0.1:8501
```

Se a porta `8501` estiver ocupada:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\start_dashboard.ps1 -Refresh -Port 8502
```

## Estrutura Principal do Repositório

- [streamlit_app.py](streamlit_app.py): interface principal do painel
- [farmlab/analysis.py](farmlab/analysis.py): regras analíticas e síntese dos dados
- [farmlab/database.py](farmlab/database.py): persistência e leitura do banco local
- [farmlab/io.py](farmlab/io.py): descoberta e ingestão dos arquivos brutos
- [scripts/start_dashboard.ps1](scripts/start_dashboard.ps1): inicialização assistida do projeto

## Arquivos Auxiliares

- [season_mapping.example.csv](templates/season_mapping.example.csv)
- [costs.example.csv](templates/costs.example.csv)

## Limitações Atuais

Apesar de já oferecer uma base consistente para exploração e comparação, o projeto ainda apresenta limitações importantes:

- os recortes de NDVI não trazem o nome oficial do talhão;
- o pacote atual contém imagens JPG e metadados, mas não os TIFFs numéricos originais;
- a análise de solo não possui coordenadas ou chave direta de talhão;
- a estação meteorológica representa o contexto geral da fazenda, e não a variabilidade local fina;
- a análise econômica depende do fornecimento de planilhas complementares de custo.

## Continuidade do Trabalho

Como evolução natural, o projeto poderá incorporar:

- mapeamento oficial entre recortes de NDVI e talhões;
- inclusão estruturada de custos por hectare e por operação;
- comparação econômica direta entre manejo convencional e tecnologia 4.0;
- ampliação do módulo analítico para modelos estatísticos e de aprendizado de máquina.

Dessa forma, o repositório se posiciona como uma base acadêmica aplicada, com potencial de expansão para análises mais robustas e apresentações técnicas mais completas.
