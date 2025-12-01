unit uPrincipal;

{$mode objfpc}{$H+}

interface

uses
  Classes, SysUtils, Forms, Controls, Graphics, Dialogs, StdCtrls, ExtCtrls,
  uDM, IniFiles;

type

  { TfrmPrincipal }

  TfrmPrincipal = class(TForm)
    memLog: TMemo;
    Timer: TTimer;
    procedure FormShow(Sender: TObject);
    procedure TimerTimer(Sender: TObject);
  private
    procedure AddMessage(Value: String);
    function LerConfiguracao(Path: String; Campo: String): String;
  public

  end;

var
  frmPrincipal: TfrmPrincipal;

implementation

{$R *.lfm}

{ TfrmPrincipal }

procedure TfrmPrincipal.TimerTimer(Sender: TObject);
var
  DM: TDM;
begin
  Timer.Enabled := False;
  memLog.Clear;
  AddMessage('Início');
  DM := TDM.Create(nil);
  try
    try
      DM.Conectar(ExtractFilePath(Application.ExeName) + 'Job.ini');

      DM.Gerar;

      DM.Desconectar;
    except
      on Erro: Exception do
      begin
        AddMessage(Erro.Message);
      end;
    end;
  finally
    FreeAndNil(DM);
    AddMessage('Fim');
    Timer.Enabled := True;
  end;
end;

procedure TfrmPrincipal.FormShow(Sender: TObject);
var
  Path: String;
begin
  Path := ExtractFilePath(Application.ExeName) + 'Job.ini';
  Timer.Interval := StrToIntDef(LerConfiguracao(Path, 'Timer'), 60) * 1000;

  Timer.Enabled := True;
end;

procedure TfrmPrincipal.AddMessage(Value: String);
begin
  memLog.Append(FormatDateTime('yyyy-mm-dd hh:nn:ss', Now) + ' - ' + Value);
end;

function TfrmPrincipal.LerConfiguracao(Path: String; Campo: String): String;
var
  Ini: TIniFile;
begin
  Ini := TIniFile.Create(Path);
  try
    Result := Ini.ReadString('Configuracao', Campo, '');
  finally
    Ini.Free;
  end;
end;

end.

