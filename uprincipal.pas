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
    Timer1: TTimer;
    procedure FormShow(Sender: TObject);
    procedure Timer1Timer(Sender: TObject);
  private
    procedure AddMessage(Value: String);
  public

  end;

var
  frmPrincipal: TfrmPrincipal;

implementation

{$R *.lfm}

{ TfrmPrincipal }

procedure TfrmPrincipal.Timer1Timer(Sender: TObject);
begin
  Timer1.Enabled := False;
  AddMessage('Início');
  try
    try
      DM.Conectar(ExtractFilePath(Application.ExeName) + 'Job.ini');

      DM.Desconectar;
    except
      on Erro: Exception do
      begin
        AddMessage(Erro.Message);
      end;
    end;
  finally
    AddMessage('Fim');
    Timer1.Enabled := True;
  end;
end;

procedure TfrmPrincipal.FormShow(Sender: TObject);
begin
  Timer1.Enabled := True;
end;

procedure TfrmPrincipal.AddMessage(Value: String);
begin
  memLog.Append(FormatDateTime('yyyy-mm-dd hh:nn:ss', Date) + ' - ' + Value);
end;

end.

