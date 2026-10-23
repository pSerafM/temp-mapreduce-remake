# Correlações de temperatura com MapReduce

O projeto calcula a correlação de Pearson entre `temp` (coluna 9 do CSV, índice
8 em Python) e todas as outras variáveis do dataset Forest Fires. O arquivo
incluído tem 517 registros, 13 campos por registro e não possui cabeçalho.

## Execução local

Na raiz do projeto, com Python 3:

```powershell
python src\run_local.py
```

Também é possível informar outro CSV:

```powershell
python src\run_local.py caminho\para\forestfireinput.csv
```

## Execução no Hadoop

Em um ambiente com Hadoop Streaming, HDFS, `HADOOP_HOME` e Python 3 disponíveis
nos nós executores:

```bash
bash src/run_hadoop.sh
```

O script envia o CSV para `/user/<usuário>/forestfire/input`, executa o job e
imprime as partes de saída do reducer.

## Fluxo do código

1. **Leitura do registro:** Hadoop Streaming fornece ao mapper cada registro de
   texto pela entrada padrão (`stdin`). Com o formato deste dataset, cada
   registro ocupa uma linha; o mapper recebe os campos juntos e não reparte uma
   linha CSV entre slaves. CSVs com campos que contenham quebras de linha
   internas exigiriam uma estratégia de entrada diferente.
2. **Validação:** `parse_record` em [src/correlation.py](./src/correlation.py)
   lê o CSV respeitando as regras de aspas, exige exatamente 13 colunas,
   converte todos os números e verifica que não sejam infinitos ou `NaN`.
   Também verifica os nomes de mês e dia. A função termina a validação antes de
   devolver qualquer variável; assim uma linha incompleta não contribui
   parcialmente para as estatísticas. Um erro é escrito em `stderr` e encerra a
   tarefa com status de falha. Não se deve configurar o Hadoop para ignorar
   esse erro se a exigência é preservar a integridade dos dados.
3. **Map:** `map_line` transforma cada linha válida em pares
   `chave<TAB>valor`. A chave é o nome da variável (`RH`, `area`, `month=jan`
   etc.). O valor é o estado estatístico de um único par `(temp, valor)`, não
   uma correlação local. São criadas 10 variáveis numéricas e 19 indicadoras
   categóricas (12 meses e 7 dias): 29 resultados.
4. **Combiner:** cada combiner agrega estados com a mesma chave no nó local. Isso
   reduz muitas mensagens individuais a um estado por variável por partição.
   O formato de saída é igual ao formato que aceita como entrada. Hadoop pode
   executar o combiner zero, uma ou várias vezes, então o resultado precisa
   continuar válido se os dados já tiverem sido combinados antes.
5. **Shuffle/sort:** o framework ordena/agrupa pelas chaves emitidas pelo
   mapper e transfere cada variável para o reducer responsável por ela. A ordem
   em que as partições chegam não deve afetar a resposta matemática.
6. **Reduce:** para cada chave, o reducer agrega todos os estados parciais
   recebidos. Só depois calcula Pearson e imprime quantidade e coeficiente. Se
   a variável for constante, a divisão por zero é evitada e a correlação aparece
   como `NA`.

O fluxo local de [src/run_local.py](./src/run_local.py) imita esse processo em
blocos de 50 linhas: map, agregação local, agrupamento/ordenação simulados e
reduce final. O limite de 50 é apenas para a demonstração local; Hadoop define
suas próprias partições. Ambos chamam as mesmas funções de agregação.

## Como a agregação evita erros

`src/correlation.py` mantém, para cada variável, seis valores:

| Campo | Significado |
|---|---|
| `n` | quantidade de pares observados |
| `mean_temperature` | média das temperaturas |
| `mean_value` | média dos valores da variável |
| `M2_temperature` | soma de `(temp - média_temp)²` |
| `M2_value` | soma de `(valor - média_valor)²` |
| `C` (`co_moment`) | soma dos produtos de desvios pareados |

Para juntar estados A e B, o código calcula `n = nA + nB`, as diferenças
`delta_temp = média_temp_B - média_temp_A` e
`delta_valor = média_valor_B - média_valor_A`, e o peso
`w = nA * nB / n`. As médias combinadas são ajustadas pela proporção de cada
grupo. Os acumuladores são atualizados assim:

```text
M2_temp = M2_temp_A + M2_temp_B + delta_temp² * w
M2_valor = M2_valor_A + M2_valor_B + delta_valor² * w
C = C_A + C_B + delta_temp * delta_valor * w
```

O estado inicial de uma linha tem `n=1`, médias iguais aos valores da linha e
acumuladores de desvio iguais a zero. As atualizações do combiner/reducer
acrescentam corretamente a variação entre observações e entre grupos. Essa
forma é útil para MapReduce porque não exige transmitir nem manter na memória
todas as linhas; a combinação é feita por soma de estados parciais, com as
contagens e diferenças entre médias ponderando corretamente grupos de tamanhos
diferentes.

No fim, o reducer calcula:

```text
r = C / sqrt(M2_temperatura * M2_variável)
```

Este é o coeficiente de Pearson. Valores próximos de `+1` indicam associação
linear positiva; próximos de `-1`, associação negativa; próximos de `0`,
associação linear fraca. Correlação não prova causalidade e Pearson mede
associação linear, não qualquer tipo de relação. Para uma variável indicadora,
o valor também pode ser entendido como associação entre a temperatura e a
presença daquela categoria, mas não como ordenação das categorias.

O algoritmo combina estatísticas suficientes em vez de fazer a subtração entre
grandes somas quase iguais usada por algumas fórmulas diretas, o que tende a
ser mais estável numericamente. Ainda são números de ponto flutuante, portanto
pequenas diferenças de arredondamento entre ordens de combinação são normais.
Se um dos lados não tiver variação (`M2=0`), Pearson não está definido e a
saída mostra `NA`.

## Integridade e validação

- O dataset incluído foi inspecionado: 517 linhas, todas com 13 colunas.
- Um registro não válido faz o mapper falhar com número da linha e motivo; ele
  não transforma o problema em um resultado com menos observações sem avisar.
- Cada resultado contém a quantidade de pares usados. Neste arquivo, cada uma
  das 29 variáveis tem contagem 517.
- O código local foi executado e os 29 coeficientes foram comparados a um
  cálculo Pearson direto sobre todos os valores originais.
- A validação local não significa que um cluster Hadoop tenha sido executado.
  A execução distribuída requer Hadoop/Streaming, HDFS e Python 3 nos nós.

## Roteiro para explicar na defesa

1. Apresente a pergunta: “Como a temperatura se relaciona com cada variável?”
2. Mostre a posição de `temp` na lista de colunas e explique a diferença entre
   a coluna humana 9 e o índice 8 em Python.
3. Abra `parse_record` e mostre validação de largura, conversão numérica e
   validação das categorias. Destaque que a linha inteira é validada primeiro.
4. Abra `map_line`: descreva as chaves e o estado unitário enviado, sem afirmar
   que o mapper calcula Pearson.
5. Abra `merge_statistics`: explique `n`, as médias, os deltas e o peso
   `nA*nB/n`. O ponto principal é mostrar como combinar partições com tamanhos
   diferentes sem tirar a média de resultados parciais.
6. Abra `combiner.py`: explique a redução local do tráfego e por que seu formato
   de entrada e saída é o mesmo.
7. Abra `reducer.py`: mostre a troca de chave, o fechamento de cada grupo e a
   emissão da correlação apenas depois de agregar os estados recebidos.
8. Execute `python src/run_local.py`, aponte as contagens e interprete o sinal
   e a magnitude de um ou dois coeficientes. Reforce que associação não é causa.
9. Seja transparente sobre o ambiente: a execução local validou os cálculos; a
   execução real em Hadoop ainda precisa ser feita num cluster configurado.

### Perguntas prováveis

**Por que não calcular Pearson em cada mapper e fazer a média depois?**  
Porque partições podem ter quantidades diferentes de registros e médias de
correlações locais não são a correlação do conjunto. Os estados guardam
contagens e permitem reconstruir os termos globais antes de dividir.

**O combiner sempre executa?**  
Não. Hadoop pode não executá-lo ou pode executá-lo mais de uma vez. Por isso o
reducer aceita exatamente o mesmo estado agregado que o combiner emite.

**Como garante a integridade da linha?**  
O input do Hadoop entrega linhas completas para este CSV de uma linha por
registro. O parser valida o número de campos e todos os valores antes de emitir
as saídas daquela linha. Se detectar problema, a tarefa falha em vez de ignorar
o registro.

**Por que há 29 correlações, não 12?**  
São 10 variáveis numéricas além de temperatura, mais 12 indicadores de mês e 7
indicadores de dia: `10 + 12 + 7 = 29`.

**O resultado prova que uma variável causa aumento de temperatura?**  
Não. É uma medida de associação linear; correlação não demonstra causalidade.

## Código e arquivos

- `src/correlation.py`: validação do registro, codificação das categorias e
  funções compartilhadas para combinar estatísticas e calcular Pearson.
- `src/mapper.py`: valida e mapeia cada registro completo.
- `src/combiner.py`: agrega estados localmente nos nós.
- `src/reducer.py`: agrega estados globais e emite `quantidade` e `correlacao`.
- `src/run_local.py`: simula blocos, combiner, shuffle/sort e reducer sem Hadoop,
  usando os mesmos cálculos do job distribuído.
- `src/run_hadoop.sh`: carrega o arquivo no HDFS e executa Hadoop Streaming,
  incluindo o módulo compartilhado `correlation.py` na distribuição.

As colunas do CSV seguem a ordem:
`X,Y,month,day,FFMC,DMC,DC,ISI,temp,RH,wind,rain,area`.
