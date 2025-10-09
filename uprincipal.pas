unit uPrincipal;

{$mode objfpc}{$H+}

interface

uses
  Classes, SysUtils, Forms, Controls, Graphics, Dialogs, StdCtrls, ExtCtrls,
  uDM;

type

  { TfrmPrincipal }

  TfrmPrincipal = class(TForm)
    memLog: TMemo;
    Timer: TTimer;
    procedure FormShow(Sender: TObject);
    procedure TimerTimer(Sender: TObject);
  private
    procedure AddMessage(Value: String);
  public

  end;

var
  frmPrincipal: TfrmPrincipal;

implementation

{$R *.lfm}

{ TfrmPrincipal }

procedure TfrmPrincipal.TimerTimer(Sender: TObject);
begin
  Timer.Enabled := False;
  memLog.Clear;
  AddMessage('Início');
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
    AddMessage('Fim');
    Timer.Enabled := True;
  end;
end;

procedure TfrmPrincipal.FormShow(Sender: TObject);
begin
  Timer.Interval := StrToIntDef(DM.LerConfiguracao(ExtractFilePath(Application.ExeName) + 'Job.ini', 'Timer'), 60) * 1000;

  Timer.Enabled := True;
end;

procedure TfrmPrincipal.AddMessage(Value: String);
begin
  memLog.Append(FormatDateTime('yyyy-mm-dd hh:nn:ss', Date) + ' - ' + Value);
end;

end.

