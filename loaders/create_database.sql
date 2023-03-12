CREATE DATABASE IF NOT EXISTS `siconvdata` 
CHARACTER SET `utf8mb4`
COLLATE `utf8mb4_general_ci`;

use siconvdata;
drop table if exists calendario;
drop table if exists convenios;
drop table if exists data_atual;
drop table if exists emendas;
drop table if exists emendas_convenios;
drop table if exists fornecedores;
drop table if exists movimento;
drop table if exists municipios;
drop table if exists estados;
drop table if exists proponentes;
drop table if exists licitacoes;
drop table if exists atributos;

CREATE TABLE `calendario` (
  `DATA` date not null,
  `ANO` int not null,
  `MES_NUMERO` smallint not null,
  `MES_NOME` varchar(10) not null,
  `ANO_MES_NUMERO` int not null,
  `MES_ANO` varchar(7) not null,
  `MES_NOME_ANO` varchar(15) not null,
  `TRIMESTRE_NUMERO` smallint not null,
  `TRIMESTRE_NOME` varchar(12) not null,
  `ANO_TRIMESTRE_NUMERO` int not null,
  `TRIMESTRE_ANO` varchar(7) not null,
  `TRIMESTRE_NOME_ANO` varchar(17) not null,
  `SEMANA_NUMERO` smallint not null,
  `SEMANA_NOME` varchar(10) not null,
  `ANO_SEMANA_NUMERO` int not null,
  `SEMANA_ANO` varchar(7) not null,
  `SEMANA_NOME_ANO` varchar(15) not null,
  `DIA_DA_SEMANA` varchar(13) not null,
  PRIMARY KEY (`DATA`),
  KEY `idx_calendario_ano` (`ANO`),
  KEY `idx_calendario_ano_mes_numero` (`ANO_MES_NUMERO`),
  KEY `idx_calendario_ano_trimestre_numero` (`ANO_TRIMESTRE_NUMERO`),
  KEY `idx_calendario_ano_semana_numero` (`ANO_SEMANA_NUMERO`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE `convenios` (
  `NR_CONVENIO` int not null,
  `DIA_ASSIN_CONV` date not null,
  `SIT_CONVENIO` varchar(80) not null,
  `INSTRUMENTO_ATIVO` char(3) not null,
  `DIA_PUBL_CONV` date DEFAULT NULL,
  `DIA_INIC_VIGENC_CONV` date not null,
  `DIA_FIM_VIGENC_CONV` date not null,
  `DIA_LIMITE_PREST_CONTAS` date DEFAULT NULL,
  `VL_GLOBAL_CONV` decimal(18,2) not null,
  `VL_REPASSE_CONV` decimal(18,2) not null,
  `VL_CONTRAPARTIDA_CONV` decimal(18,2) not null,
  `COD_ORGAO_SUP` int not null,
  `DESC_ORGAO_SUP` varchar(80) DEFAULT NULL,
  `NATUREZA_JURIDICA` varchar(60) not null,
  `COD_ORGAO` int not null,
  `DESC_ORGAO` varchar(80) not null,
  `MODALIDADE` varchar(20) not null,
  `IDENTIF_PROPONENTE` varchar(14) not null,
  `OBJETO_PROPOSTA` text,
  `VALOR_EMENDA_CONVENIO` decimal(18, 2) not null,
  `COM_EMENDAS` char(3) not null,
  `INSUCESSO` float not null,
  PRIMARY KEY (`NR_CONVENIO`),
  KEY `idx_convenios_proponente` (`IDENTIF_PROPONENTE`),
  KEY `idx_convenios_dia_inic_vigenc_conv` (`DIA_INIC_VIGENC_CONV`),
  KEY `idx_convenios_dia_fim_vigenc_conv` (`DIA_FIM_VIGENC_CONV`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE `data_atual` (
  `DATA_ATUAL` date not null
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE `emendas` (
  `NR_EMENDA` bigint not null,
  `NOME_PARLAMENTAR` varchar(60) not null,
  `TIPO_PARLAMENTAR` varchar(20) not null,
  `VALOR_EMENDA` decimal(18,2) not null,
  PRIMARY KEY (`NR_EMENDA`),
  KEY `idx_parlamentar` (`NOME_PARLAMENTAR`(60),`TIPO_PARLAMENTAR`(20))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE `emendas_convenios` (
  `NR_EMENDA` bigint not null,
  `NR_CONVENIO` int not null,
  `VALOR_REPASSE_EMENDA` decimal(18,2) not null,
  KEY `idx_emendas_convenios_nr_convenio` (`NR_CONVENIO`),
  KEY `idx_nr_convenio_nr_emenda` (`NR_EMENDA`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE `fornecedores` (
  `FORNECEDOR_ID` int not null,
  `IDENTIF_FORNECEDOR` varchar(40) not null,
  `NOME_FORNECEDOR` varchar(150) not null,
  PRIMARY KEY (`FORNECEDOR_ID`),
  KEY `idx_fornecedores_identif_nome` (`IDENTIF_FORNECEDOR`,`NOME_FORNECEDOR`(60))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE `movimento` (
  `MOV_ID` bigint not null,
  `NR_CONVENIO` int not null, 
  `DATA_MOV` date not null,
  `VALOR_MOV` decimal(18,2) not null,
  `TIPO_MOV` char(1) not null, 
  `FORNECEDOR_ID` int not null,
  PRIMARY KEY (`MOV_ID`, `DATA_MOV` ),
  KEY `idx_movimento_convenio` (`NR_CONVENIO`),
  KEY `idx_movimento_fornecedor_id` (`FORNECEDOR_ID`),
  KEY `idx_movimento_data` (`DATA_MOV`),
  KEY `idx_movimento_tipo` (`TIPO_MOV`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  PARTITION BY HASH (year(`DATA_MOV`))
  PARTITIONS 20;

CREATE TABLE `municipios` (
  `CODIGO_IBGE` bigint not null,
  `NOME_MUNICIPIO` varchar(60) not null,
  `UF` char(2) not null,
  `REGIAO` varchar(12) not null,
  `REGIAO_ABREVIADA` char(2) not null,
  `NOME_ESTADO` varchar(30) not null,
  `LATITUDE` float,
  `LONGITUDE` float,
  `CAPITAL` char(3) not null,
  PRIMARY KEY (`CODIGO_IBGE`),
  KEY `idx_municipios_uf` (`UF`(2))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE `estados` (
  `SIGLA` char(2) not null,
  `NOME` varchar(30) not null,
  `REGIAO` varchar(12) not null,
  `REGIAO_ABREVIADA` char(2) not null,
  PRIMARY KEY (`SIGLA`),
  KEY `idx_estados_nome` (`NOME`(5))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE `proponentes` (
  `IDENTIF_PROPONENTE` varchar(14) not null,
  `NM_PROPONENTE` varchar(150) not null,
  `CODIGO_IBGE` bigint not null,
  PRIMARY KEY (`IDENTIF_PROPONENTE`),
  KEY `idx_proponentes_codigo_ibge` (`CODIGO_IBGE`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE `licitacoes` (
  `ID_LICITACAO` integer not null, 
  `NR_CONVENIO` integer not null, 
  `MODALIDADE_COMPRA` varchar(50) not null, 
  `TIPO_LICITACAO` varchar(20) not null, 
  `FORMA_LICITACAO` varchar(20) not null, 
  `REGISTRO_PRECOS` char(3) not null, 
  `LICITACAO_INTERNACIONAL` char(3) not null,
  `STATUS_LICITACAO` varchar(15) not null, 
  `VALOR_LICITACAO` decimal(18, 2) not null,
  PRIMARY KEY (`ID_LICITACAO`),
  KEY `idx_licitacoes_nr_convenio` (`NR_CONVENIO`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE `atributos` (
  `ATRIBUTO` varchar(30) not null, 
  `VALOR` varchar(100) not null
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;