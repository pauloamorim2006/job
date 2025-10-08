unit uDM;

{$mode ObjFPC}{$H+}

interface

uses
  Classes, SysUtils, ZConnection, ZDataset, IniFiles;

type

  { TDM }

  TDM = class(TDataModule)
    Conexao: TZConnection;
    Query: TZQuery;
  private

  public
    procedure Conectar(Path: String);
    procedure Desconectar;
  end;

var
  DM: TDM;

implementation

{$R *.lfm}

{ TDM }

procedure TDM.Conectar(Path: String);
var
  Ini: TIniFile;
begin
  Ini := TIniFile.Create(Path);
  try
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

end.

