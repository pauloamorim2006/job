unit uDM;

{$mode ObjFPC}{$H+}

interface

uses
  Classes, SysUtils, ACBrSpedFiscal, ZConnection, ZDataset, ZAbstractRODataset,
  IniFiles, ACBrEFDBlocos, ACBrUtil.Strings, StrUtils, DateUtils, Forms, Math,
  Dialogs;

type

  { TDM }

  TDM = class(TDataModule)
    ACBrSPEDFiscal: TACBrSPEDFiscal;
    Conexao: TZConnection;
    QryC170: TZQuery;
    QryC190: TZQuery;
    QryC500: TZQuery;
    QryC590: TZQuery;
    QryC101: TZQuery;
    QryD500: TZQuery;
    QryD590: TZQuery;
    QryE210RetencaoST: TZQuery;
    QryE210RessarcST: TZQuery;
    QryE210CredST: TZQuery;
    QryE520Deb: TZQuery;
    QryE520Cred: TZQuery;
    QryTotalCredito: TZQuery;
    Query: TZQuery;
    QryEmpresa: TZQuery;
    QryContador: TZQuery;
    QryParticipante: TZQuery;
    QryProduto: TZQuery;
    QryMovimentacao: TZQuery;
    QryC100NFe: TZQuery;
    QryTotalDebito: TZQuery;
    QryE200: TZQuery;
    QryE210DevolvST: TZQuery;
    QryE510: TZQuery;
    QryEstoque: TZQuery;
    Transaction: TZTransaction;
    QryExec: TZQuery;
  private
    FDataInicial: TDateTime;
    FDataFinal: TDateTime;
    FPerfil: String;
    FCaminhoSpedFiscal: String;
    FLocal: String;
    FErro: Boolean;
    FMensagem: String;

    function AnoToVersao: TACBrCodVer;
    function PossuiMovimentacao: Boolean;
    procedure AdicionarProdutoAo0200(Codigo: Integer; AliquotaIcms: Double = 0);

    function GetSituacao(Situacao: Integer; FinalidadeEmissao: Integer): TACBrCodSit;
    function GetIndicadorEmitente(TipoNfe: Integer): TACBrIndEmit;
    function GetIndicadorOperacao(Codigo: Integer): TACBrIndOper;
    function GetIndPagamento(TipoFinanceiro: String): TACBrIndPgto;
    function GetCSTICMSFormatado(CST: String): String;
    function RemoverQuebrasDeLinha(const Texto: string; Substituto: string = ''): string;
    function StrToDataISO(const S: string): TDateTime;
    procedure FecharTodasAsTZQuery;

    procedure GerarBloco0;
    procedure GerarBlocoC;
    procedure GerarBlocoD;
    procedure GerarBlocoE;
    procedure GerarBlocoG;
    procedure GerarBlocoH;
    procedure GerarBlocoK;
    procedure GerarBloco1;
    procedure GerarArquivo;
    procedure Atualizar;
  public
    procedure Conectar(Path: String);
    procedure Desconectar;
    procedure Gerar;

    function LerConfiguracao(Path: String; Campo: String): String;
  end;

var
  DM: TDM;

implementation

{$R *.lfm}

{ TDM }

function TDM.AnoToVersao: TACBrCodVer;
var
  xVer: string;
begin
  if (FDataInicial >= StrToDate('01/01/2009')) and (FDataInicial <= StrToDate('31/12/2009')) then
    xVer := '002'
  else if (FDataInicial >= StrToDate('01/01/2010')) and (FDataInicial <= StrToDate('31/12/2010')) then
    xVer := '003'
  else if (FDataInicial >= StrToDate('01/01/2011')) and (FDataInicial <= StrToDate('31/12/2011')) then
    xVer := '004'
  else if (FDataInicial >= StrToDate('01/01/2012')) and (FDataInicial <= StrToDate('30/06/2012')) then
    xVer := '005'
  else if (FDataInicial >= StrToDate('01/07/2012')) and (FDataInicial <= StrToDate('31/12/2012')) then
    xVer := '006'
  else if (FDataInicial >= StrToDate('01/01/2013')) and (FDataInicial <= StrToDate('31/12/2013')) then
    xVer := '007'
  else if (FDataInicial >= StrToDate('01/01/2014')) and (FDataInicial <= StrToDate('31/12/2014')) then
    xVer := '008'
  else if (FDataInicial >= StrToDate('01/01/2015')) and (FDataInicial <= StrToDate('31/12/2015')) then
    xVer := '009'
  else if (FDataInicial >= StrToDate('01/01/2016')) and (FDataInicial <= StrToDate('31/12/2016')) then
    xVer := '010'
  else if (FDataInicial >= StrToDate('01/01/2017')) and (FDataInicial <= StrToDate('31/12/2017')) then
    xVer := '011'
  else if (FDataInicial >= StrToDate('01/01/2018')) and (FDataInicial <= StrToDate('31/12/2018')) then
    xVer := '012'
  else if (FDataInicial >= StrToDate('01/01/2019')) and (FDataInicial <= StrToDate('31/12/2019')) then
    xVer := '013'
  else if (FDataInicial >= StrToDate('01/01/2020')) and (FDataInicial <= StrToDate('31/12/2020')) then
    xVer := '014'
  else if (FDataInicial >= StrToDate('01/01/2021')) and (FDataInicial <= StrToDate('31/12/2021')) then
    xVer := '015'
  else if (FDataInicial >= StrToDate('01/01/2022')) and (FDataInicial <= StrToDate('31/12/2022')) then
    xVer := '016'
  else if (FDataInicial >= StrToDate('01/01/2023')) and (FDataInicial <= StrToDate('31/12/2023')) then
    xVer := '017'
  else if (FDataInicial >= StrToDate('01/01/2024')) and (FDataInicial <= StrToDate('31/12/2024')) then
    xVer := '018'
  else if (FDataInicial >= StrToDate('01/01/2025')) and (FDataInicial <= StrToDate('31/12/2025')) then
    xVer := '019';
  Result := StrToCodVer(xVer);
end;

function TDM.PossuiMovimentacao: Boolean;
begin
  QryMovimentacao.Close;
  QryMovimentacao.SQL.Text :=
    'SELECT 1 FROM NotasFiscais(NOLOCK)                                                                                                ' +
    'WHERE Situacao IN (1, 2, 3, 4) AND                                                                                                ' +
    'CAST((CASE WHEN TipoNfe = 2 THEN COALESCE(DataHoraSaida, DataHoraEmissao) ELSE DataHoraEmissao END) AS DATE) BETWEEN :DE AND :ATE ' +
    'AND LicencaId = :LicencaId AND EmpresaId = :EmpresaId                                                                             ' +
    IfThen(Query.FieldByName('GerarH').AsBoolean or (MonthOf(FDataInicial) = 2),
    'UNION                                                                                                                             ' +
    'SELECT 1 FROM ProdutosServicos(NOLOCK) WHERE LicencaId = :LicencaId                                                               ',
    EmptyStr
    );
  QryMovimentacao.ParamByName('DE').AsDate := FDataInicial;
  QryMovimentacao.ParamByName('ATE').AsDate := FDataFinal;
  QryMovimentacao.ParamByName('LicencaId').AsInteger := Query.FieldByName('LicencaId').AsInteger;
  QryMovimentacao.ParamByName('EmpresaId').AsInteger := Query.FieldByName('EmpresaId').AsInteger;
  QryMovimentacao.Open;

  Result := not QryMovimentacao.IsEmpty;
end;

procedure TDM.AdicionarProdutoAo0200(Codigo: Integer; AliquotaIcms: Double = 0);
begin
  QryProduto.Close;
  QryProduto.ParamByName('Id').AsInteger := Codigo;
  QryProduto.Open;

  if not ACBrSpedFiscal.Bloco_0.Registro0001.Registro0200.LocalizaRegistro(trim(QryProduto.FieldByName('CodigoInterno').AsString)) then
  begin

    if QryProduto.fieldByName('Sigla').AsString <> EmptyStr then
    begin
      if not ACBrSpedFiscal.Bloco_0.Registro0001.Registro0190.LocalizaRegistro(trim(QryProduto.fieldByName('Sigla').AsString)) then
      begin
        with ACBrSpedFiscal.Bloco_0.Registro0001.Registro0190.New do
        begin
          UNID  := QryProduto.fieldByName('Sigla').AsString;
          DESCR := QryProduto.fieldByName('UnidadeDescricao').AsString;
        end;
      end;
    end;

    with ACBrSpedFiscal.Bloco_0.Registro0001.Registro0200.New do
    begin
      COD_ITEM := QryProduto.FieldByName('CodigoInterno').AsString;
      DESCR_ITEM := QryProduto.FieldByName('Nome').AsString;
      COD_BARRA := QryProduto.FieldByName('Gtin').AsString;
      UNID_INV := Trim(QryProduto.FieldByName('Sigla').AsString);
      TIPO_ITEM := StrToTipoItem(Trim(QryProduto.FieldByName('TipoItem').AsString));
      COD_NCM := Trim(QryProduto.FieldByName('Ncm').AsString);
      COD_GEN := EmptyStr;
      ALIQ_ICMS := AliquotaIcms;
    end;
  end;
end;

function TDM.GetSituacao(Situacao: Integer; FinalidadeEmissao: Integer): TACBrCodSit;
begin
  if (FinalidadeEmissao = 2) then
    Result := sdFiscalCompl
  else if (Situacao = 4) then
    Result := sdDoctoNumInutilizada
  else if (Situacao = 2) then
    Result := sdCancelado
  else if (Situacao = 3) then
    Result := sdDoctoDenegado
  else
    Result := sdRegular;
end;

function TDM.GetIndicadorEmitente(TipoNfe: Integer): TACBrIndEmit;
begin
  case TipoNfe of
    2: Result := edTerceiros;
    else Result := edEmissaoPropria;
  end;
end;

function TDM.GetIndicadorOperacao(Codigo: Integer): TACBrIndOper;
begin
  case Codigo of
    0, 2: Result := tpEntradaAquisicao;
    else Result := tpSaidaPrestacao;
  end;
end;

function TDM.GetIndPagamento(TipoFinanceiro: String): TACBrIndPgto;
begin
  Result := tpPrazo;
  if TipoFinanceiro = 'A' then
    Result := tpVista;
end;

function TDM.GetCSTICMSFormatado(CST: String): String;
begin
  if Copy(CST, 2, 2) = '11' then
    Result := Copy(CST, 1, 1) + '10'
  else if Copy(CST, 2, 2) = '42' then
    Result := Copy(CST, 1, 1) + '41'
  else if Copy(CST, 2, 2) = '91' then
    Result := Copy(CST, 1, 1) + '90'
  else
    Result := CST;
end;

procedure TDM.GerarBloco0;
var
  AAno, AMes: Integer;
begin
  AAno := Query.FieldByName('Ano').AsInteger;
  AMes := Query.FieldByName('Mes').AsInteger;
  FPerfil := Query.FieldByName('Perfil').AsString;

  FDataInicial := EncodeDate(AAno, AMes, 1);
  FDataFinal := IncMonth(FDataInicial, 1) - 1;

  ACBrSPEDFiscal.DT_INI := FDataInicial;
  ACBrSPEDFiscal.DT_FIN := FDataFinal;

  QryEmpresa.Close;
  QryEmpresa.ParamByName('Id').AsInteger := Query.FieldByName('EmpresaId').AsInteger;
  QryEmpresa.ParamByName('LicencaId').AsInteger := Query.FieldByName('LicencaId').AsInteger;
  QryEmpresa.Open;

  with ACBrSPEDFiscal.Bloco_0 do
  begin

    {$REGION '0000: ABERTURA DO ARQUIVO DIGITAL E IDENTIFICAÇÃO DA ENTIDADE'}
    with Registro0000New do
    begin
      COD_VER := AnoToVersao;
      COD_FIN := StrToCodFin(Query.FieldByName('Finalidade').AsString);
      NOME := QryEmpresa.FieldByName('Nome').AsString;
      CNPJ := QryEmpresa.FieldByName('CnpjCpfDi').AsString;
      CPF := EmptyStr;
      UF := QryEmpresa.FieldByName('Uf').AsString;
      IE := OnlyNumber(Trim(QryEmpresa.FieldByName('InscricaoEstadual').AsString));
      if QryEmpresa.FieldByName('CodigoIbge').IsNull then
        raise Exception.Create('Código do Município da empresa é de preenchimento obrigatório!');
      COD_MUN := QryEmpresa.FieldByName('CodigoIbge').AsInteger;
      IM := EmptyStr;
      SUFRAMA := EmptyStr;
      IND_PERFIL := StrToIndPerfil(Query.FieldByName('Perfil').AsString);
      IND_ATIV := StrToIndAtiv(Query.FieldByName('TipoAtividade').AsString);
    end;
    {$ENDREGION}

    with Registro0001New do
    begin

      {$REGION 'Identifica Movimentação'}
      IND_MOV := imComDados;
      if not PossuiMovimentacao then
        IND_MOV := imSemDados;
      {$ENDREGION}

      if Trim(Query.FieldByName('Registro002').AsString) <> EmptyStr then
      begin
        with Registro0002New do
        begin
          CLAS_ESTAB_IND := Trim(Query.FieldByName('Registro002').AsString);
        end;
      end;

      {$REGION '0005 - DADOS COMPLEMENTARES DA ENTIDADE'}
      with Registro0005New do
      begin
        FANTASIA := QryEmpresa.FieldByName('Fantasia').AsString;
        CEP := OnlyNumber(QryEmpresa.FieldByName('Cep').AsString);
        ENDERECO := QryEmpresa.FieldByName('Endereco').AsString;
        NUM := QryEmpresa.FieldByName('Numero').AsString;
        COMPL := QryEmpresa.FieldByName('Complemento').AsString;
        BAIRRO := QryEmpresa.FieldByName('Bairro').AsString;
        FONE := OnlyNumber(QryEmpresa.FieldByName('Telefone').AsString);
        FAX := OnlyNumber(QryEmpresa.FieldByName('Telefone').AsString);
        EMAIL := QryEmpresa.FieldByName('Email').AsString;
      end;
      {$ENDREGION}

      {$REGION '0100 - DADOS DO CONTABILISTA'}
      if Query.FieldByName('Perfil').AsString <> 'C' then
      begin

        if not QryEmpresa.FieldByName('ContadorId').IsNull then
        begin
          QryContador.Close;
          QryContador.ParamByName('Id').AsInteger :=
            QryEmpresa.FieldByName('ContadorId').AsInteger;
          QryContador.Open;

          if not QryContador.IsEmpty then
          begin
            with Registro0100New do
            begin
              NOME := QryContador.FieldByName('Nome').AsString;
              if Length(OnlyNumber(QryContador.FieldByName('CnpjCpfDi').AsString)) > 11 then
                CNPJ := OnlyNumber(QryContador.FieldByName('CnpjCpfDi').AsString)
              else
                CPF := OnlyNumber(QryContador.FieldByName('CnpjCpfDi').AsString);
              CRC := QryContador.FieldByName('InscricaoEstadual').AsString;
              CEP := OnlyNumber(QryContador.FieldByName('Cep').AsString);
              ENDERECO := QryContador.FieldByName('Endereco').AsString;
              NUM := QryContador.FieldByName('Numero').AsString;
              COMPL := QryContador.FieldByName('Complemento').AsString;
              BAIRRO := QryContador.FieldByName('Bairro').AsString;
              FONE := QryContador.FieldByName('Telefone').AsString;
              //FAX := QryContador.FieldByName('PRT_TELEFONE_2').AsString;
              EMAIL := QryContador.FieldByName('Email').AsString;
              if QryContador.FieldByName('CodigoIbge').IsNull then
                raise Exception.Create('Código do Município do contador é de preenchimento obrigatório!');
              COD_MUN := QryContador.FieldByName('CodigoIbge').AsInteger;
            end;
          end;
        end;

      end;
      {$ENDREGION}

      {$REGION '0150 - TABELA DE CADASTRO DO PARTICIPANTE'}
      QryParticipante.Close;
      QryParticipante.ParamByName('DE').AsDate := FDataInicial;
      QryParticipante.ParamByName('ATE').AsDate := FDataFinal;
      QryParticipante.ParamByName('LicencaId').AsInteger := Query.FieldByName('LicencaId').AsInteger;
      QryParticipante.ParamByName('EmpresaId').AsInteger := Query.FieldByName('EmpresaId').AsInteger;
      QryParticipante.Open;

      try
        QryParticipante.First;

        while not QryParticipante.Eof do
        begin
          if not Registro0150.LocalizaRegistro(QryParticipante.FieldByName('Id').AsString) then
          begin
            with Registro0150New do
            begin
              if QryParticipante.FieldByName('LCodigoIbge').AsString = EmptyStr then
                raise Exception.Create('Cliente sem código da cidade: ' + QryParticipante.FieldByName('Id').AsString);
              COD_PART := QryParticipante.FieldByName('Codigo').AsString;
              NOME := QryParticipante.FieldByName('Nome').AsString;
              if Length(OnlyNumber(Trim(QryParticipante.FieldByName('CnpjCpfDi').AsString))) > 11 then
                CNPJ := OnlyNumber(Trim(QryParticipante.FieldByName('CnpjCpfDi').AsString))
              else
                CPF := OnlyNumber(Trim(QryParticipante.FieldByName('CnpjCpfDi').AsString));
              COD_MUN := 9999999;
              IF (not QryParticipante.FieldByName('LPaisId').IsNull) AND
                 (QryParticipante.FieldByName('LUf').AsString = 'EX') THEN
              begin
                COD_PAIS := QryParticipante.FieldByName('LPaisId').AsString;
              end
              else
              begin
                COD_MUN := QryParticipante.FieldByName('LCodigoIbge').AsInteger;
                COD_PAIS := '1058';
              end;
              IE := OnlyNumber(Trim(QryParticipante.FieldByName('InscricaoEstadual').AsString));
              SUFRAMA := EmptyStr;
              ENDERECO := QryParticipante.FieldByName('LEndereco').AsString;
              NUM := QryParticipante.FieldByName('LNumero').AsString;
              COMPL := QryParticipante.FieldByName('LComplemento').AsString;
              BAIRRO := QryParticipante.FieldByName('LBairro').AsString;
            end;
          end;
          QryParticipante.Next;
        end;
      finally
        FreeAndNil(QryParticipante);
      end;


      {$ENDREGION}

    end;
  end;
end;

procedure TDM.GerarBlocoC;
var
  NumeroItem: Integer;
begin

  with ACBrSPEDFiscal.Bloco_C do
  begin
    with RegistroC001New do
    begin

      {$REGION 'VERIFICA MOVIMENTO'}

      QryC100NFe.Close;
      QryC100NFe.ParamByName('DE').AsDate := FDataInicial;
      QryC100NFe.ParamByName('ATE').AsDate := FDataFinal;
      QryC100NFe.ParamByName('EmpresaId').AsInteger := Query.FieldByName('EmpresaId').AsInteger;
      QryC100NFe.ParamByName('LicencaId').AsInteger := Query.FieldByName('LicencaId').AsInteger;
      QryC100NFe.Open;

      QryC500.Close;
      QryC500.ParamByName('DE').AsDate := FDataInicial;
      QryC500.ParamByName('ATE').AsDate := FDataFinal;
      QryC500.ParamByName('EmpresaId').AsInteger := Query.FieldByName('EmpresaId').AsInteger;
      QryC500.ParamByName('LicencaId').AsInteger := Query.FieldByName('LicencaId').AsInteger;
      QryC500.Open;

      IND_MOV := imSemDados;
      if ((not QryC100NFe.IsEmpty) or
        (not QryC500.IsEmpty)) then
      begin
        IND_MOV := imComDados;
      end;

      {$ENDREGION}

      {$REGION 'C100 - NOTA FISCAL (CÓDIGO 01), NOTA FISCAL AVULSA (CÓDIGO 1B), NOTA FISCAL DE PRODUTOR (CÓDIGO 04), NF-e (CÓDIGO 55) e NFC-e (CÓDIGO 65)'}

      QryC100NFe.First;

      while not QryC100NFe.Eof do
      begin

        with RegistroC100New do
        begin
          COD_SIT := GetSituacao(
            QryC100NFe.FieldByName('Situacao').AsInteger,
            QryC100NFe.FieldByName('Finalidade').AsInteger);
          IND_EMIT := GetIndicadorEmitente(
            QryC100NFe.FieldByName('TipoNfe').AsInteger);
          COD_MOD := QryC100NFe.FieldByName('Modelo').AsString;
          NUM_DOC := Trim(QryC100NFe.FieldByName('Numero').AsString);
          CHV_NFE := QryC100NFe.FieldByName('Chave').AsString;

          {if (not (QryC100NFe.FieldByName('NFE_FINNFE').AsInteger in [2])) then
          begin}
            IND_OPER := GetIndicadorOperacao
              (QryC100NFe.FieldByName('TipoNfe').AsInteger);
            SER := Trim(QryC100NFe.FieldByName('Serie').AsString);
          //end;

          {$REGION 'NOTAS FISCAIS FATURADAS'}
          if QryC100NFe.FieldByName('Situacao').AsInteger = 1 then
          begin
            DT_DOC := StrToDataISO(QryC100NFe.FieldByName('DataHoraEmissao').AsString);
            COD_PART := QryC100NFe.FieldByName('PessoaCodigo').AsString;

            {$REGION 'NOTA FISCAL COMPLEMENTAR NÃO PRECISA DE INFORMAR ESTES DADOS'}
            //if (not (QryC100NFe.FieldByName('Finalidade').AsInteger in [2])) then
            begin
              DT_E_S := StrToDataISO(QryC100NFe.FieldByName('DataHoraSaida').AsString);
              if ((QryC100NFe.FieldByName('DataHoraSaida').IsNull) or (StrToDataISO(QryC100NFe.FieldByName('DataHoraSaida').AsString) <= 0)) then
                DT_E_S := DT_DOC;
              IND_PGTO := GetIndPagamento(QryC100NFe.FieldByName('TipoFinanceiro').AsString);
              IND_FRT := StrToIndFrt(QryC100NFe.FieldByName('TipoFrete').AsString);
              VL_DOC := QryC100NFe.FieldByName('ValorTotal').AsFloat;
              VL_DESC := QryC100NFe.FieldByName('ValorDesconto').AsFloat;
              { Campo não configurado }
              VL_ABAT_NT := 0;
              VL_MERC := QryC100NFe.FieldByName('ValorProduto').AsFloat;
              VL_SEG := QryC100NFe.FieldByName('ValorSeguro').AsFloat;
              VL_OUT_DA := QryC100NFe.FieldByName('ValorOutro').AsFloat;
              VL_BC_ICMS := QryC100NFe.FieldByName('ValorBaseIcms').AsFloat;
              VL_ICMS := QryC100NFe.FieldByName('ValorIcms').AsFloat;
              VL_BC_ICMS_ST := QryC100NFe.FieldByName('ValorBCST').AsFloat;
              VL_ICMS_ST := QryC100NFe.FieldByName('ValorICMSST').AsFloat;
              VL_IPI := QryC100NFe.FieldByName('ValorIpi').AsFloat;
              VL_PIS := QryC100NFe.FieldByName('ValorPis').AsFloat;
              VL_COFINS := QryC100NFe.FieldByName('ValorCofins').AsFloat;
              VL_PIS_ST := 0;
              VL_COFINS_ST := 0;
              VL_FRT := QryC100NFe.FieldByName('ValorFrete').AsFloat;

              {$REGION 'C101 - DIFAL'}

              if Query.FieldByName('DifalAjustes').AsBoolean then
              begin
                if QryC100NFe.FieldByName('TipoNfe').AsInteger in [1] then
                begin
                  QryC101.Close;
                  QryC101.ParamByName('NotaFiscalId').AsInteger :=
                    QryC100NFe.FieldByName('Id').AsInteger;
                  QryC101.Open;

                  if not QryC101.IsEmpty then
                  begin
                    if ((QryC101.FieldByName('DifalVFCPUFDest').AsFloat +
                      QryC101.FieldByName('DifalVICMSUFDest').AsFloat +
                      QryC101.FieldByName('DifalVICMSUFRemet').AsFloat) > 0) then
                    begin
                      if not QryC101.IsEmpty then
                      begin
                        with RegistroC101New do
                        begin
                          VL_FCP_UF_DEST := QryC101.FieldByName('DifalVFCPUFDest').AsFloat;
                          VL_ICMS_UF_DEST := QryC101.FieldByName('DifalVICMSUFDest').AsFloat;
                          VL_ICMS_UF_REM := QryC101.FieldByName('DifalVICMSUFRemet').AsFloat;
                        end;
                      end;
                    end;
                  end;

                end;
              end;

              {$ENDREGION 'C101 - DIFAL'}

              {$REGION 'Perfil A e B'}

              if ((FPerfil = 'A') or (FPerfil = 'B')) then
              begin

                {$REGION 'Notas Fiscais de Entrada'}

                if (QryC100NFe.FieldByName('TipoNfe').AsInteger in [2]) or
                   (Query.FieldByName('GerarC170Saida').AsBoolean) then
                begin

                  {$REGION 'C170 - ITENS DO DOCUMENTO (CÓDIGO 01, 1B, 04 e 55)'}
                  QryC170.Close;
                  QryC170.ParamByName('Id').AsInteger :=
                    QryC100NFe.FieldByName('Id').AsInteger;
                  QryC170.Open;

                  NumeroItem := 1;

                  while not QryC170.Eof do
                  begin

                    AdicionarProdutoAo0200(
                      QryC170.FieldByName('ProdutoId').AsInteger,
                      QryC170.FieldByName('AliquotaIcms').AsFloat);

                    with RegistroC170New do
                    begin
                      NUM_ITEM := IntToStr(NumeroItem);
                      COD_ITEM := QryC170.FieldByName('ProdutoCodigo').AsString;
                      DESCR_COMPL := Copy(RemoverQuebrasDeLinha(QryC170.FieldByName('InformacaoAdicional').AsString), 1, 100);
                      QTD := QryC170.FieldByName('Quantidade').AsFloat;
                      UNID := Trim(QryC170.FieldByName('Sigla').AsString);
                      VL_ITEM := QryC170.FieldByName('ValorProduto').AsFloat;
                      VL_DESC := QryC170.FieldByName('ValorDesconto').AsFloat;
                      IND_MOV := mfNao;
                      if QryC100NFe.FieldByName('Finalidade').AsInteger in [1] then
                        IND_MOV := mfSim;
                      if (Length(Trim(QryC170.FieldByName('Cst').AsString)) = 3) then
                        CST_ICMS := Trim(QryC170.FieldByName('Cst').AsString)
                      else
                        CST_ICMS := GetCSTICMSFormatado(
                          QryC170.FieldByName('Origem').AsString +
                          QryC170.FieldByName('Cst').AsString);
                      CFOP := QryC170.FieldByName('Cfop').AsString;
                      COD_NAT := EmptyStr;
                      VL_BC_ICMS := QryC170.FieldByName('ValorBaseIcms').AsFloat;
                      ALIQ_ICMS := QryC170.FieldByName('AliquotaIcms').AsFloat;
                      VL_ICMS := QryC170.FieldByName('ValorIcms').AsFloat;
                      VL_BC_ICMS_ST := 0;
                      ALIQ_ST := 0;
                      VL_ICMS_ST := 0;
                      if QryC170.FieldByName('ValorICMSST').AsFloat > 0 then
                      begin
                        VL_BC_ICMS_ST :=
                          QryC170.FieldByName('ValorBCST').AsFloat;
                        ALIQ_ST :=
                          QryC170.FieldByName('PercentualICMSST').AsFloat;
                        VL_ICMS_ST :=
                          QryC170.FieldByName('ValorICMSST').AsFloat;
                      end;
                      IND_APUR := iaMensal;
                      CST_IPI :=
                        QryC170.FieldByName('IpiCst').AsString;
                      VL_BC_IPI :=
                        QryC170.FieldByName('ValorBaseIpi').AsFloat;
                      ALIQ_IPI :=
                        QryC170.FieldByName('AliquotaIpi').AsFloat;
                      VL_IPI :=
                        QryC170.FieldByName('ValorIpi').AsFloat;
                      CST_PIS :=
                        QryC170.FieldByName('PisCst').AsString;
                      VL_BC_PIS := 0;
                      ALIQ_PIS_PERC := 0;
                      QUANT_BC_PIS := 0;
                      ALIQ_PIS_R := 0;
                      VL_BC_PIS :=
                        QryC170.FieldByName('ValorBasePis').AsFloat;
                      ALIQ_PIS_PERC :=
                        QryC170.FieldByName('AliquotaPis').AsFloat;
                      VL_PIS :=
                        QryC170.FieldByName('ValorPis').AsFloat;
                      CST_COFINS :=
                        QryC170.FieldByName('CofinsCst').AsString;
                      VL_BC_COFINS := 0;
                      ALIQ_COFINS_PERC := 0;
                      QUANT_BC_COFINS := 0;
                      ALIQ_COFINS_R := 0;
                      VL_BC_COFINS := QryC170.FieldByName('ValorBaseCofins').AsFloat;
                      ALIQ_COFINS_PERC := QryC170.FieldByName('AliquotaCofins').AsFloat;
                      VL_COFINS :=
                        QryC170.FieldByName('ValorCofins').AsFloat;
                      COD_CTA := EmptyStr;

                    end; // Fim dos Itens;

                    Inc(NumeroItem);
                    QryC170.Next;
                  end;
                  {$ENDREGION}

                end;

                {$ENDREGION}

              end;

              {$ENDREGION}

            end;

            {$ENDREGION}

            {$REGION 'C190: REGISTRO ANALÍTICO DO DOCUMENTO (CÓDIGO 01, 1B, 04, 55 e 65)'}
            QryC190.Close;
            QryC190.ParamByName('Id').AsInteger :=
              QryC100NFe.FieldByName('Id').AsInteger;
            QryC190.Open;

            QryC190.First;
            while not QryC190.Eof do
            begin

              with RegistroC190New do
              begin
                CST_ICMS :=
                  GetCSTICMSFormatado(
                  QryC190.FieldByName('CST').AsString);
                CFOP := QryC190.FieldByName('Cfop').AsString;
                ALIQ_ICMS := QryC190.FieldByName('AliquotaIcms').AsFloat;
                VL_OPR := QryC190.FieldByName('ValorTotal').AsFloat;
                VL_BC_ICMS := QryC190.FieldByName('ValorBaseIcms').AsFloat;
                VL_ICMS := QryC190.FieldByName('ValorIcms').AsFloat;
                VL_BC_ICMS_ST := QryC190.FieldByName('ValorBCST').AsFloat;
                VL_ICMS_ST := QryC190.FieldByName('ValorICMSST').AsFloat;
                VL_RED_BC := QryC190.FieldByName('Reducao').AsFloat;
                VL_IPI := QryC190.FieldByName('ValorIpi').AsFloat;
                COD_OBS := EmptyStr;

              end;

              QryC190.Next;
            end;
            {$ENDREGION}

            {$REGION 'C195: AJUSTES DIFAL'}
            if Query.FieldByName('DifalAjustes').AsBoolean then
            begin
              if IND_OPER in [tpSaidaPrestacao] then
              begin
                QryC101.Close;
                QryC101.ParamByName('NotaFiscalId').AsInteger :=
                  QryC100NFe.FieldByName('Id').AsInteger;
                QryC101.Open;

                if ((QryC101.FieldByName('DifalVFCPUFDest').AsFloat +
                  QryC101.FieldByName('DifalVICMSUFDest').AsFloat +
                  QryC101.FieldByName('DifalVICMSUFRemet').AsFloat) > 0) then
                begin

                  {$REGION 'C197: AJUSTES DIFAL'}
                  QryC170.Close;
                  QryC170.ParamByName('Id').AsInteger :=
                    QryC100NFe.FieldByName('Id').AsInteger;
                  QryC170.Open;

                  QryC170.First;
                  with RegistroC195New do
                  begin
                    COD_OBS := 'DIFAL';

                    while not QryC170.Eof do
                    begin
                      if (QryC170.FieldByName('DifalVICMSUFDest').AsFloat > 0) then
                      begin
                        AdicionarProdutoAo0200(
                          QryC170.FieldByName('ProdutoId').AsInteger,
                          QryC170.FieldByName('AliquotaIcms').AsFloat);

                        with RegistroC197New do
                        begin
                          COD_AJ := Query.FieldByName('CodigoAjusteDifal').AsString;
                          COD_ITEM := QryC170.FieldByName('ProdutoCodigo').AsString;
                          VL_BC_ICMS := QryC170.FieldByName('DifalVBCUFDest').AsFloat;

                          if (QryC170.FieldByName('DifalPICMSInter').AsFloat >=
                            QryC170.FieldByName('DifalPICMSUFDest').AsFloat) then
                          begin
                            ALIQ_ICMS :=
                              QryC170.FieldByName('DifalPICMSInter').AsFloat -
                              QryC170.FieldByName('DifalPICMSUFDest').AsFloat;
                          end
                          else
                          begin
                            ALIQ_ICMS :=
                              QryC170.FieldByName('DifalPICMSUFDest').AsFloat -
                              QryC170.FieldByName('DifalPICMSInter').AsFloat;
                          end;
                          VL_ICMS := QryC170.FieldByName('DifalVICMSUFDest').AsFloat;
                          VL_OUTROS := 0;
                        end;
                      end;
                      QryC170.Next;
                    end;
                  end;
                  {$ENDREGION}

                end;
              end;

            end;
            {$ENDREGION}

          end;
          {$ENDREGION}

        end;

        QryC100NFe.Next;
      end;

      {$ENDREGION}

      {$REGION 'C500 - NOTA FISCAL/CONTA DE ENERGIA ELÉTRICA (CÓDIGO 06), NOTA FISCAL/CONTA DE FORNECIMENTO D'ÁGUA CANALIZADA (CÓDIGO 29) E NOTA FISCAL CONSUMO FORNECIMENTO DE GÁS (CÓDIGO 28)'}

      QryC500.First;

      while not QryC500.Eof do
      begin
        with RegistroC500New do
        begin
          COD_SIT := GetSituacao(
            QryC500.FieldByName('Situacao').AsInteger,
            QryC500.FieldByName('Finalidade').AsInteger);
          IND_OPER := GetIndicadorOperacao
              (QryC100NFe.FieldByName('TipoNfe').AsInteger);
          IND_EMIT := GetIndicadorEmitente(
            QryC100NFe.FieldByName('TipoNfe').AsInteger);
          COD_MOD := QryC500.FieldByName('Modelo').AsString;
          SER := Trim(QryC500.FieldByName('Serie').AsString);
          NUM_DOC := (QryC500.FieldByName('Numero').AsString);
          DT_DOC := StrToDataISO(QryC500.FieldByName('DataHoraEmissao').AsString);

          if QryC100NFe.FieldByName('Situacao').AsInteger = 1 then
          begin
            COD_PART := QryC500.FieldByName('PessoaCodigo').AsString;

            if (not (QryC500.FieldByName('Finalidade').AsInteger in [2])) then
            begin
              SUB := EmptyStr;
              DT_E_S := StrToDataISO(QryC500.FieldByName('DataHoraSaida').AsString);
              VL_DOC := QryC500.FieldByName('ValorTotal').AsFloat;
              VL_DESC := QryC500.FieldByName('ValorDesconto').AsFloat;
              VL_FORN := QryC500.FieldByName('ValorTotal').AsFloat;
              VL_SERV_NT := 0;
              VL_TERC := 0;
              VL_DA := QryC500.FieldByName('ValorOutro').AsFloat;
              VL_BC_ICMS := QryC500.FieldByName('ValorBaseIcms').AsFloat;
              VL_ICMS := QryC500.FieldByName('ValorIcms').AsFloat;
              VL_BC_ICMS_ST := QryC500.FieldByName('ValorBCST').AsFloat;
              VL_ICMS_ST := QryC500.FieldByName('ValorICMSST').AsFloat;
              COD_INF := EmptyStr;
              VL_PIS := QryC500.FieldByName('ValorPis').AsFloat;
              VL_COFINS := QryC500.FieldByName('ValorCofins').AsFloat;
              if ((QryC500.FieldByName('TipoNfe').AsInteger in [1]) and
                (QryC500.FieldByName('Modelo').AsString = '06')) then
              begin
                TP_LIGACAO := tlNenhum;
                COD_GRUPO_TENSAO := gtNenhum;
              end;
              COD_CONS := EmptyStr;

              {$REGION 'C590 - REGISTRO ANALÍTICO DO DOCUMENTO - NOTA FISCAL/CONTA DE ENERGIA ELÉTRICA (CÓDIGO 06), NOTA FISCAL/CONTA DE FORNECIMENTO D'ÁGUA CANALIZADA (CÓDIGO 29) E NOTA FISCAL CONSUMO FORNECIMENTO DE GÁS (CÓDIGO 28)'}

              QryC590.Close;
              QryC590.ParamByName('Id').AsInteger :=
                QryC500.FieldByName('Id').AsInteger;
              QryC590.Open;

              QryC590.First;

              while not QryC590.Eof do
              begin
                with RegistroC590New do
                begin
                  CST_ICMS :=
                    GetCSTICMSFormatado(
                    QryC590.FieldByName('Cst').AsString);
                  CFOP :=
                    QryC590.FieldByName('Cfop').AsString;
                  ALIQ_ICMS :=
                    QryC590.FieldByName('AliquotaIcms').AsFloat;
                  VL_OPR :=
                    QryC590.FieldByName('ValorTotal').AsFloat;
                  VL_BC_ICMS :=
                    QryC590.FieldByName('ValorBaseIcms').AsFloat;
                  VL_ICMS :=
                    QryC590.FieldByName('ValorIcms').AsFloat;
                  VL_BC_ICMS_ST :=
                    QryC590.FieldByName('ValorBCST').AsFloat;
                  VL_ICMS_ST :=
                    QryC590.FieldByName('ValorICMSST').AsFloat;
                  VL_RED_BC :=
                    QryC590.FieldByName('Reducao').AsFloat;
                  COD_OBS :=
                    EmptyStr;
                end;

                QryC590.Next;
              end;

              {$ENDREGION}

            end;

          end;

        end;

        QryC500.Next;
      end;

      {$ENDREGION}

      {****************
      Do registro 600 a 800 são somente registros que não tem função
      Energia Elétrica, Gás, Água e CF-e de São Paulo
      ****************}

    end;
  end;

end;

procedure TDM.GerarBlocoD;
begin
  QryD500.Close;
  QryD500.ParamByName('DE').AsDate := FDataInicial;
  QryD500.ParamByName('ATE').AsDate := FDataFinal;
  QryD500.ParamByName('EmpresaId').AsInteger := Query.FieldByName('EmpresaId').AsInteger;
  QryD500.ParamByName('LicencaId').AsInteger := Query.FieldByName('LicencaId').AsInteger;
  QryD500.Open;

  with ACBrSPEDFiscal.Bloco_D do
  begin
    with RegistroD001New do
    begin

      {$REGION 'MOVIMENTAÇÃO DE DADOS'}
      IND_MOV := imSemDados;
      if (not QryD500.IsEmpty) then
        IND_MOV := imComDados;
      {$ENDREGION}

      {$REGION 'D500 - NOTA FISCAL DE SERVIÇO DE COMUNICAÇÃO (CÓDIGO 21) E NOTA FISCAL DE SERVIÇO DE TELECOMUNICAÇÃO (CÓDIGO 22)'}

      QryD500.First;

      while not QryD500.Eof do
      begin
        with RegistroD500New do
        begin
          IND_OPER := GetIndicadorOperacao(QryD500.FieldByName('TipoNfe').AsInteger);
          IND_EMIT := GetIndicadorEmitente(
            QryD500.FieldByName('TipoNfe').AsInteger);
          COD_MOD := QryD500.FieldByName('Modelo').AsString;
          COD_SIT := GetSituacao(
            QryD500.FieldByName('Situacao').AsInteger,
            QryD500.FieldByName('Finalidade').AsInteger);
          SER := Trim(QryD500.FieldByName('Serie').AsString);
          NUM_DOC := Trim(QryD500.FieldByName('Numero').AsString);
          DT_DOC := StrToDataISO(QryD500.FieldByName('DataHoraEmissao').AsString);

          {$REGION 'FATURADAS'}
          if QryC100NFe.FieldByName('Situacao').AsInteger = 1 then
          begin
            SUB := EmptyStr;
            DT_A_P := StrToDataISO(QryD500.FieldByName('DataHoraSaida').AsString);
            COD_PART := QryD500.FieldByName('PessoaCodigo').AsString;
            VL_DOC := QryD500.FieldByName('ValorTotal').AsFloat;
            VL_DESC := QryD500.FieldByName('ValorDesconto').AsFloat;
            VL_SERV := QryD500.FieldByName('ValorTotal').AsFloat;
            VL_SERV_NT := 0;
            VL_TERC := 0;
            VL_DA := QryD500.FieldByName('ValorOutro').AsFloat;
            VL_BC_ICMS := QryD500.FieldByName('ValorBaseIcms').AsFloat;
            VL_ICMS := QryD500.FieldByName('ValorIcms').AsFloat;
            COD_INF := EmptyStr;
            VL_PIS := QryD500.FieldByName('ValorPis').AsFloat;
            VL_COFINS := QryD500.FieldByName('ValorCofins').AsFloat;
            TP_ASSINANTE := assComercialIndustrial;

            {$REGION 'D590 - REGISTRO ANALÍTICO DO DOCUMENTO (CÓDIGO 21 E 22)'}

            QryD590.Close;
            QryD590.ParamByName('Id').AsInteger :=
              QryD500.FieldByName('Id').AsInteger;
            QryD590.Open;

            QryD590.First;

            while not QryD590.Eof do
            begin
              with RegistroD590New do
              begin
                CST_ICMS :=
                  GetCSTICMSFormatado(
                  QryD590.FieldByName('Cst').AsString);
                CFOP := QryD590.FieldByName('Cfop').AsString;
                ALIQ_ICMS := QryD590.FieldByName('AliquotaIcms').AsFloat;
                VL_OPR := QryD590.FieldByName('ValorTotal').AsFloat;
                VL_BC_ICMS := QryD590.FieldByName('ValorBaseIcms').AsFloat;
                VL_ICMS := QryD590.FieldByName('ValorIcms').AsFloat;
                VL_BC_ICMS_UF := 0;
                { Verificar este campo }
                VL_ICMS_UF := 0;
                if ((Copy(CST_ICMS, 2, 2) = '20') or
                  (Copy(CST_ICMS, 2, 2) = '70') or
                  (Copy(CST_ICMS, 2, 2) = '90')) then
                begin
                  VL_RED_BC := QryD590.FieldByName('Reducao').AsFloat;
                end;
                COD_OBS := EmptyStr;
              end;

              QryD590.Next;
            end;

            {$ENDREGION}

          end;
          {$ENDREGION}

        end;

        QryD500.Next;
      end;

      {$ENDREGION}

    end;
  end;
end;

procedure TDM.GerarBlocoE;
var
  Resultado: Double;
begin
  with ACBrSPEDFiscal.Bloco_E do
  begin
    with RegistroE001New do
    begin
      IND_MOV := imComDados;

      {$REGION 'E100'}

      with RegistroE100New do
      begin
        DT_INI := FDataInicial;
        DT_FIN := FDataFinal;

        {$REGION 'E110'}

        with RegistroE110New do
        begin
          QryTotalDebito.Close;
          QryTotalDebito.ParamByName('DE').AsDate := FDataInicial;
          QryTotalDebito.ParamByName('ATE').AsDate := FDataFinal;
          QryTotalDebito.ParamByName('EmpresaId').AsInteger := Query.FieldByName('EmpresaId').AsInteger;
          QryTotalDebito.ParamByName('LicencaId').AsInteger := Query.FieldByName('LicencaId').AsInteger;
          QryTotalDebito.Open;

          QryTotalCredito.Close;
          QryTotalCredito.ParamByName('DE').AsDate := FDataInicial;
          QryTotalCredito.ParamByName('ATE').AsDate := FDataFinal;
          QryTotalCredito.ParamByName('EmpresaId').AsInteger := Query.FieldByName('EmpresaId').AsInteger;
          QryTotalCredito.ParamByName('LicencaId').AsInteger := Query.FieldByName('LicencaId').AsInteger;
          QryTotalCredito.Open;

          VL_TOT_DEBITOS := QryTotalDebito.FieldByName('Valor').AsFloat;
          VL_AJ_DEBITOS := 0;
          VL_TOT_AJ_DEBITOS := 0;
          VL_ESTORNOS_CRED := 0;
          VL_TOT_CREDITOS := QryTotalCredito.FieldByName('Valor').AsFloat;
          VL_AJ_CREDITOS := 0;
          VL_TOT_AJ_CREDITOS := 0;
          VL_ESTORNOS_DEB := 0;
          VL_SLD_CREDOR_ANT := 0;
          VL_SLD_APURADO :=
            VL_TOT_DEBITOS +
            VL_AJ_DEBITOS +
            VL_TOT_AJ_DEBITOS +
            VL_ESTORNOS_CRED -
            VL_TOT_CREDITOS -
            VL_AJ_CREDITOS -
            VL_TOT_AJ_CREDITOS -
            VL_ESTORNOS_DEB -
            VL_SLD_CREDOR_ANT;
          if VL_SLD_APURADO >= 0 then
          begin
            VL_SLD_CREDOR_TRANSPORTAR := 0;
          end
          else
          begin
            VL_SLD_CREDOR_TRANSPORTAR :=
              VL_SLD_APURADO * -1;
            VL_SLD_APURADO := 0;
          end;
          VL_TOT_DED := 0;
          VL_ICMS_RECOLHER :=
            VL_SLD_APURADO -
            VL_TOT_DED;
          if VL_ICMS_RECOLHER < 0 then
          begin
            VL_SLD_CREDOR_TRANSPORTAR :=
              VL_ICMS_RECOLHER * -1;
            VL_ICMS_RECOLHER := 0
          end;
          DEB_ESP := 0;
        end;

        {$ENDREGION}

      end;

      {$ENDREGION}

      {$REGION 'E200'}

      QryE200.Close;
      QryE200.ParamByName('DE').AsDate := FDataInicial;
      QryE200.ParamByName('ATE').AsDate := FDataFinal;
      QryE200.ParamByName('EmpresaId').AsInteger := Query.FieldByName('EmpresaId').AsInteger;
      QryE200.ParamByName('LicencaId').AsInteger := Query.FieldByName('LicencaId').AsInteger;
      QryE200.Open;

      while not QryE200.Eof do
      begin
        with RegistroE200New do
        begin
          DT_INI := FDataInicial;
          DT_FIN := FDataFinal;
          UF := QryE200.FieldByName('ESTADO').AsString;

          {$REGION 'E210'}

          with RegistroE210New do
          begin
            QryE210DevolvST.Close;
            QryE210DevolvST.ParamByName('DE').AsDate := FDataInicial;
            QryE210DevolvST.ParamByName('ATE').AsDate := FDataFinal;
            QryE210DevolvST.ParamByName('EmpresaId').AsInteger := Query.FieldByName('EmpresaId').AsInteger;
            QryE210DevolvST.ParamByName('LicencaId').AsInteger := Query.FieldByName('LicencaId').AsInteger;
            QryE210DevolvST.ParamByName('ESTADO').AsString := QryE200.FieldByName('ESTADO').AsString;
            QryE210DevolvST.Open;

            QryE210RessarcST.Close;
            QryE210RessarcST.ParamByName('DE').AsDate := FDataInicial;
            QryE210RessarcST.ParamByName('ATE').AsDate := FDataFinal;
            QryE210RessarcST.ParamByName('EmpresaId').AsInteger := Query.FieldByName('EmpresaId').AsInteger;
            QryE210RessarcST.ParamByName('LicencaId').AsInteger := Query.FieldByName('LicencaId').AsInteger;
            QryE210RessarcST.ParamByName('ESTADO').AsString := QryE200.FieldByName('ESTADO').AsString;
            QryE210RessarcST.Open;

            QryE210CredST.Close;
            QryE210CredST.ParamByName('DE').AsDate := FDataInicial;
            QryE210CredST.ParamByName('ATE').AsDate := FDataFinal;
            QryE210CredST.ParamByName('EmpresaId').AsInteger := Query.FieldByName('EmpresaId').AsInteger;
            QryE210CredST.ParamByName('LicencaId').AsInteger := Query.FieldByName('LicencaId').AsInteger;
            QryE210CredST.ParamByName('ESTADO').AsString := QryE200.FieldByName('ESTADO').AsString;
            QryE210CredST.Open;

            QryE210RetencaoST.Close;
            QryE210RetencaoST.ParamByName('DE').AsDate := FDataInicial;
            QryE210RetencaoST.ParamByName('ATE').AsDate := FDataFinal;
            QryE210RetencaoST.ParamByName('EmpresaId').AsInteger := Query.FieldByName('EmpresaId').AsInteger;
            QryE210RetencaoST.ParamByName('LicencaId').AsInteger := Query.FieldByName('LicencaId').AsInteger;
            QryE210RetencaoST.ParamByName('ESTADO').AsString := QryE200.FieldByName('ESTADO').AsString;
            QryE210RetencaoST.Open;

            IND_MOV_ST := mstComOperacaoST;
            VL_SLD_CRED_ANT_ST := 0;
            VL_DEVOL_ST := QryE210DevolvST.FieldByName('ValorICMSST').AsFloat;
            VL_RESSARC_ST := QryE210RessarcST.FieldByName('ValorICMSST').AsFloat;
            VL_OUT_CRED_ST := QryE210CredST.FieldByName('ValorICMSST').AsFloat;
            VL_AJ_CREDITOS_ST := 0;
            VL_RETENCAO_ST := QryE210RetencaoST.FieldByName('ValorICMSST').AsFloat;
            VL_OUT_DEB_ST := 0;
            VL_AJ_DEBITOS_ST := 0;
            VL_SLD_DEV_ANT_ST :=
              VL_RETENCAO_ST +
              VL_OUT_DEB_ST +
              VL_AJ_DEBITOS_ST -
              VL_SLD_CRED_ANT_ST -
              VL_DEVOL_ST -
              VL_RESSARC_ST -
              VL_OUT_CRED_ST -
              VL_AJ_CREDITOS_ST;
            VL_SLD_CRED_ST_TRANSPORTAR := 0;
            if VL_SLD_DEV_ANT_ST < 0 then
            begin
              VL_SLD_CRED_ST_TRANSPORTAR :=
                VL_SLD_DEV_ANT_ST * -1;
              VL_SLD_DEV_ANT_ST := 0;
            end;
            VL_DEDUCOES_ST := 0;
            { Somatório de E250 }
            VL_ICMS_RECOL_ST :=
              VL_SLD_DEV_ANT_ST -
              VL_DEDUCOES_ST;
            { Somatório de E250 }
            DEB_ESP_ST := 0;

          end;

          {$ENDREGION}

        end;
        QryE200.Next;
      end;

      {$ENDREGION}

      {$REGION 'IPI'}

      if ACBrSPEDFiscal.Bloco_0.Registro0000.IND_ATIV = atIndustrial then
      begin

        {$REGION 'E500'}
        with RegistroE500New do
        begin
          IND_APUR := iaMensal;
          DT_INI := FDataInicial;
          DT_FIN := FDataFinal;

          {$REGION 'E510'}

          QryE510.Close;
          QryE510.ParamByName('DE').AsDate := FDataInicial;
          QryE510.ParamByName('ATE').AsDate := FDataFinal;
          QryE510.ParamByName('EmpresaId').AsInteger := Query.FieldByName('EmpresaId').AsInteger;
          QryE510.ParamByName('LicencaId').AsInteger := Query.FieldByName('LicencaId').AsInteger;
          QryE510.Open;

          while not QryE510.Eof do
          begin
            with RegistroE510New do
            begin
              CFOP := QryE510.FieldByName('Cfop').AsString;
              CST_IPI := QryE510.FieldByName('IpiCst').AsString;
              VL_CONT_IPI := QryE510.FieldByName('ValorBaseIpi').AsFloat;
              VL_BC_IPI := QryE510.FieldByName('ValorBaseIpi').AsFloat;
              VL_IPI := QryE510.FieldByName('ValorIpi').AsFloat;
            end;

            QryE510.Next;
          end;

          {$ENDREGION}

          {$REGION 'E520'}

          with RegistroE520New do
          begin
            QryE520Deb.Close;
            QryE520Deb.ParamByName('DE').AsDate := FDataInicial;
            QryE520Deb.ParamByName('ATE').AsDate := FDataFinal;
            QryE520Deb.ParamByName('EmpresaId').AsInteger := Query.FieldByName('EmpresaId').AsInteger;
            QryE520Deb.ParamByName('LicencaId').AsInteger := Query.FieldByName('LicencaId').AsInteger;
            QryE520Deb.Open;

            QryE520Cred.Close;
            QryE520Cred.ParamByName('DE').AsDate := FDataInicial;
            QryE520Cred.ParamByName('ATE').AsDate := FDataFinal;
            QryE520Cred.ParamByName('EmpresaId').AsInteger := Query.FieldByName('EmpresaId').AsInteger;
            QryE520Cred.ParamByName('LicencaId').AsInteger := Query.FieldByName('LicencaId').AsInteger;
            QryE520Cred.Open;

            VL_SD_ANT_IPI := 0;
            VL_DEB_IPI := QryE520Deb.FieldByName('Valor').AsFloat;
            VL_CRED_IPI := QryE520Cred.FieldByName('Valor').AsFloat;
            VL_OD_IPI := 0;
            VL_OC_IPI := 0;
            Resultado :=
              VL_DEB_IPI +
              VL_OD_IPI -
              VL_SD_ANT_IPI -
              VL_CRED_IPI -
              VL_OC_IPI;
            if Resultado < 0 then
            begin
              VL_SC_IPI := Resultado * -1;
              VL_SD_IPI := 0;
            end
            else
            begin
              VL_SC_IPI := 0;
              VL_SD_IPI := Resultado;
            end;
          end;
          {$ENDREGION}

        end;
        {$ENDREGION}

      end;
      {$ENDREGION}

    end;
  end;
end;

procedure TDM.GerarBlocoG;
begin
  with ACBrSPEDFiscal.Bloco_G do
  begin
    with RegistroG001New do
    begin
      IND_MOV := imSemDados;
    end;
  end;
end;

procedure TDM.GerarBlocoH;
begin
  with ACBrSPEDFiscal.Bloco_H do
  begin

    {$REGION 'Bloco H001'}
    with RegistroH001New do
    begin
      IND_MOV := imSemDados;

      {$REGION 'Fevereiro'}

      if (MonthOf(FDataInicial) = 2) or (Query.FieldByName('GerarH').AsBoolean) then
      begin

        IND_MOV := imComDados;

        {$REGION 'Bloco H005'}
        with RegistroH005New do
        begin
          if MonthOf(FDataInicial) = 2 then
            DT_INV := EndOfTheYear(IncYear(FDataInicial, -1))
          else
            DT_INV := FDataFinal;
          VL_INV := 0;
          if MonthOf(FDataInicial) = 2 then
            MOT_INV := miFinalPeriodo
          else
            MOT_INV := miDeterminacaoFiscos;

          {$REGION 'Bloco H010'}

          QryEstoque.Close;
          QryEstoque.ParamByName('Data').AsDate := DT_INV;
          QryEstoque.ParamByName('EmpresaId').AsInteger := Query.FieldByName('EmpresaId').AsInteger;
          QryEstoque.ParamByName('LicencaId').AsInteger := Query.FieldByName('LicencaId').AsInteger;
          QryEstoque.Open;
          QryEstoque.First;

          while not QryEstoque.Eof do
          begin
            AdicionarProdutoAo0200(QryEstoque.FieldByName('Id').AsInteger);

            with RegistroH010New do
            begin
              COD_ITEM := QryEstoque.FieldByName('CodigoInterno').AsString;
              UNID := Trim(QryEstoque.FieldByName('Sigla').AsString);
              QTD := QryEstoque.FieldByName('QuantidadeEstoque').AsFloat;
              VL_UNIT := QryEstoque.FieldByName('PrecoCusto').AsFloat;
              VL_ITEM := QTD * VL_UNIT;
              VL_INV := VL_INV + VL_ITEM;
              IND_PROP := piInformante;
              COD_PART := EmptyStr;
              TXT_COMPL := EmptyStr;
              COD_CTA := EmptyStr;
            end;

            QryEstoque.Next;
          end;

          {$ENDREGION}
        end;
        {$ENDREGION}

      end;

      {$ENDREGION}

    end;
    {$ENDREGION}

  end;
end;

procedure TDM.GerarBlocoK;
begin
  with ACBrSPEDFiscal.Bloco_K do
  begin
    with RegistroK001New do
    begin
      IND_MOV := imSemDados;
      if Query.FieldByName('BlocoK').AsBoolean then
      begin
        IND_MOV := imComDados;

        with RegistroK100New do
        begin
          DT_INI := ACBrSPEDFiscal.DT_INI;
          DT_FIN := ACBrSPEDFiscal.DT_FIN;

          QryEstoque.Close;
          QryEstoque.ParamByName('Data').AsDate := DT_FIN;
          QryEstoque.ParamByName('EmpresaId').AsInteger := Query.FieldByName('EmpresaId').AsInteger;
          QryEstoque.ParamByName('LicencaId').AsInteger := Query.FieldByName('LicencaId').AsInteger;
          QryEstoque.Open;
          QryEstoque.First;

          while not QryEstoque.Eof do
          begin
            try
              if (QryEstoque.FieldByName('TipoItem').AsString = '00') or
                (QryEstoque.FieldByName('TipoItem').AsString = '01') or
                (QryEstoque.FieldByName('TipoItem').AsString = '03') or
                (QryEstoque.FieldByName('TipoItem').AsString = '04') or
                (QryProduto.FieldByName('TipoItem').AsString = '05') or
                (QryEstoque.FieldByName('TipoItem').AsString = '06') then
              begin
                AdicionarProdutoAo0200(QryEstoque.FieldByName('Id').AsInteger);

                with RegistroK200New do
                begin
                  COD_ITEM := QryEstoque.FieldByName('CodigoInterno').AsString;
                  QTD := QryEstoque.FieldByName('QuantidadeEstoque').AsFloat;
                  IND_EST := estPropInformantePoder;
                  COD_PART := '';
                  DT_EST := ACBrSPEDFiscal.DT_FIN;
                end;
              end;
            finally
              FreeAndNil(QryProduto);
            end;

            QryEstoque.Next;
          end;

        end;
      end;
    end;
  end;
end;

procedure TDM.GerarBloco1;
begin
  with ACBrSPEDFiscal.Bloco_1 do
  begin
    with Registro1001New do
    begin
      IND_MOV := imComDados;

      with Registro1010New do
      begin
        IND_EXP := 'N'; // Reg. 1100 - Ocorreu averbação (conclusão) de exportação no período:
        IND_CCRF := 'N'; // Reg. 1200 – Existem informações acerca de créditos de ICMS a serem controlados, definidos pela Sefaz:
        IND_COMB := 'N'; // Reg. 1300 – É comercio varejista de combustíveis:
        IND_USINA := 'N'; // Reg. 1390 – Usinas de açúcar e/álcool – O estabelecimento é produtor de açúcar e/ou álcool carburante:
        IND_VA := 'N'; // Reg. 1400 – Existem informações a serem prestadas neste registro e o registro é obrigatório em sua Unidade da Federação:
        IND_EE := 'N'; // Reg. 1500 - A empresa é distribuidora de energia e ocorreu fornecimento de energia elétrica para consumidores de outra UF:
        IND_CART := 'N'; // Reg. 1600 - Realizou vendas com Cartão de Crédito ou de débito:
        IND_FORM := 'N'; // Reg. 1700 - É obrigatório em sua unidade da federação o controle de utilização de documentos  fiscais em papel:
        IND_AER := 'N'; // Reg. 1800 – A empresa prestou serviços de transporte aéreo de cargas e de passageiros:
        IND_GIAF1 := 'N';
        IND_GIAF3 := 'N';
        IND_GIAF4 := 'N';
        IND_REST_RESSARC_COMPL_ICMS := 'N';
      end;
    end;
  end;

end;

procedure TDM.GerarArquivo;
var
  Codigo: TGUID;
  sCodigo: String;
begin
  ACBrSPEDFiscal.LinhasBuffer := 1000;

  CreateGUID(Codigo);
  sCodigo := GUIDToString(Codigo);
  sCodigo := StringReplace(sCodigo, '{', '', [rfReplaceAll]);
  sCodigo := StringReplace(sCodigo, '}', '', [rfReplaceAll]);

  ACBrSPEDFiscal.Path := FCaminhoSpedFiscal;
  ACBrSPEDFiscal.Arquivo :=
    'SpedFiscal_' +
    Query.FieldByName('LicencaId').AsString + '_' +
    Query.FieldByName('EmpresaId').AsString + '_' +
    FormatDateTime('yyyymm', FDataInicial) + '_' +
    sCodigo + '.txt';

  FLocal := ACBrSPEDFiscal.Path + ACBrSPEDFiscal.Arquivo;

  ACBrSPEDFiscal.SaveFileTXT;


end;

procedure TDM.Atualizar;
var
  Lista: TStrings;
begin
  try
    if not Conexao.InTransaction then
      Conexao.StartTransaction;

    QryExec.Close;
    QryExec.SQL.Text :=
      'UPDATE SpedsFiscais SET ' +
      'Situacao = :Situacao,   ' +
      'Mensagem = :Mensagem,   ' +
      'FileName = :FileName,   ' +
      'Content = :Content      ' +
      'WHERE Id = :Id          ';
    QryExec.ParamByName('Situacao').AsInteger := IfThen(FErro, 3, 2);
    QryExec.ParamByName('Mensagem').AsString := IfThen(FMensagem <> EmptyStr, FMensagem, EmptyStr);
    QryExec.ParamByName('FileName').AsString := FLocal;
    QryExec.ParamByName('Content').AsString := EmptyStr;
    if ((not FErro) and FileExists(FLocal)) then
    begin
      Lista := TStringList.Create;
      try
        Lista.LoadFromFile(FLocal);
        QryExec.ParamByName('Content').AsString := Lista.Text;
      finally
        Lista.Free;
      end;
    end;
    QryExec.ParamByName('Id').AsInteger := Query.FieldByName('Id').AsInteger;
    QryExec.ExecSQL;

    if Conexao.InTransaction then
      Conexao.Commit;
  except
    on Erro: Exception do
    begin
      if Conexao.InTransaction then
        Conexao.Rollback;

      raise Exception.Create('Erro: ' + Erro.Message);
    end;
  end;
end;

procedure TDM.Conectar(Path: String);
var
  Ini: TIniFile;
begin
  Ini := TIniFile.Create(Path);
  try
    FCaminhoSpedFiscal := Ini.ReadString('Path', 'CaminhoSpedFiscal', '');

    Conexao.Disconnect;
    Conexao.Protocol := Ini.ReadString('ConexaoSQL', 'Protocol', 'ado');
    Conexao.HostName := Ini.ReadString('ConexaoSQL', 'HostName', '');
    Conexao.Database := Ini.ReadString('ConexaoSQL', 'Database', '');
    Conexao.User := Ini.ReadString('ConexaoSQL', 'User', '');
    Conexao.Password := Ini.ReadString('ConexaoSQL', 'Password', '');
    Conexao.Port := Ini.ReadInteger('ConexaoSQL', 'Port', 1433);
    Conexao.Connect;
  finally
    Ini.Free;
  end;
end;

procedure TDM.Desconectar;
begin
  if Conexao.Connected then
    Conexao.Disconnect;
end;

procedure TDM.Gerar;
begin
  try
    Query.Close;
    Query.SQL.Text := 'SELECT * FROM SpedsFiscais(NOLOCK) WHERE Situacao = 4';
    Query.Open;

    while not Query.Eof do
    begin
      try
        FErro := False;
        FMensagem := EmptyStr;
        FLocal := EmptyStr;

        GerarBloco0;
        GerarBlocoC;
        GerarBlocoD;
        GerarBlocoE;
        GerarBlocoG;
        GerarBlocoH;
        GerarBlocoK;
        GerarBloco1;
        GerarArquivo;
        Atualizar;

      except
        on Erro: Exception do
        begin
          FErro := True;
          FMensagem := 'Erro: ' + Erro.Message;
          Atualizar;
        end;
      end;

      Query.Next;
    end;

    FecharTodasAsTZQuery;
  finally
    Query.Close;
  end;
end;

function TDM.LerConfiguracao(Path: String; Campo: String): String;
var
  Ini: TIniFile;
begin
  Ini := TIniFile.Create(Path);
  try
    FCaminhoSpedFiscal := Ini.ReadString('Configuracao', Campo, '');
  finally
    Ini.Free;
  end;
end;

function TDM.RemoverQuebrasDeLinha(const Texto: string; Substituto: string = ''): string;
begin
  Result := StringReplace(Texto, #13#10, Substituto, [rfReplaceAll]);
  Result := StringReplace(Result, #13, Substituto, [rfReplaceAll]);
  Result := StringReplace(Result, #10, Substituto, [rfReplaceAll]);
end;

function TDM.StrToDataISO(const S: string): TDateTime;
var
  Ano, Mes, Dia: Word;
begin
  if Trim(S) = EmptyStr then
  begin
     Result := 0;
     Exit;
  end;

  Ano := StrToInt(Copy(S, 1, 4));
  Mes := StrToInt(Copy(S, 6, 2));
  Dia := StrToInt(Copy(S, 9, 2));
  Result := EncodeDate(Ano, Mes, Dia);
end;

procedure TDM.FecharTodasAsTZQuery;
var
  I: Integer;
begin
  for I := 0 to ComponentCount - 1 do
  begin
    if Components[I] is TZQuery then
      (Components[I] as TZQuery).Close;
  end;
end;


end.

