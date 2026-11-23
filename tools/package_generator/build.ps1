param([string]$OutFile)
$ErrorActionPreference = 'Stop'
$here = Split-Path -Parent $MyInvocation.MyCommand.Path
$refs = @(
  'C:\Windows\Microsoft.NET\assembly\GAC_MSIL\Microsoft.SqlServer.ManagedDTS\v4.0_15.0.0.0__89845dcd8080cc91\Microsoft.SqlServer.ManagedDTS.dll',
  'C:\Windows\Microsoft.NET\assembly\GAC_MSIL\Microsoft.SqlServer.DTSPipelineWrap\v4.0_15.0.0.0__89845dcd8080cc91\Microsoft.SQLServer.DTSPipelineWrap.dll',
  'C:\Windows\Microsoft.NET\assembly\GAC_64\Microsoft.SqlServer.DTSRuntimeWrap\v4.0_15.0.0.0__89845dcd8080cc91\Microsoft.SqlServer.DTSRuntimeWrap.dll',
  'C:\Windows\Microsoft.NET\assembly\GAC_64\Microsoft.SqlServer.SQLTask\v4.0_15.0.0.0__89845dcd8080cc91\Microsoft.SqlServer.SQLTask.dll',
  'System.Xml.dll', 'System.Core.dll'
)
Add-Type -Path $refs[0..3]
Add-Type -TypeDefinition (Get-Content "$here\PkgBuilder.cs" -Raw) -ReferencedAssemblies $refs -Language CSharp

$raw = Join-Path $here 'ETL_BATCH_SQL_STG.raw.dtsx'
[PkgBuilder]::Build($raw)
$log = [PkgBuilder]::Log.ToString(); $log
$tmpSql = ([regex]::Match($log, 'TMP_SQL_ID=(\{[^}]+\})')).Groups[1].Value
$tmpOra = ([regex]::Match($log, 'TMP_ORA_ID=(\{[^}]+\})')).Groups[1].Value

# ---- Post-process: point to project connection managers, keep original package DTSID ----
$projSql = '{7DEA3E2C-2990-497F-96D4-E32EB23613EC}'; $projOra = '{045001BE-1809-47F1-81D1-80CC395F6877}'
$origPkgId = '{8A6ECB4B-C617-4207-A035-79AB0C972D73}'
$x = New-Object Xml.XmlDocument; $x.PreserveWhitespace = $true; $x.Load($raw)
$ns = New-Object Xml.XmlNamespaceManager $x.NameTable; $ns.AddNamespace('DTS','www.microsoft.com/SqlServer/Dts')
foreach ($n in 'TMP_CM_SQL','TMP_CM_ORA') {
  $node = $x.SelectSingleNode("/DTS:Executable/DTS:ConnectionManagers/DTS:ConnectionManager[@DTS:ObjectName='$n']", $ns)
  if (-not $node) { throw "Temp CM $n not found" }
  [void]$node.ParentNode.RemoveChild($node)
}
$x.DocumentElement.SetAttribute('DTSID','www.microsoft.com/SqlServer/Dts', $origPkgId)
$xml = $x.OuterXml
$map = @(
  @("connectionManagerID=`"Package.ConnectionManagers[TMP_CM_SQL]`"", "connectionManagerID=`"$($projSql):external`""),
  @("connectionManagerRefId=`"Package.ConnectionManagers[TMP_CM_SQL]`"", "connectionManagerRefId=`"Project.ConnectionManagers[CM_SQLSERVER_DBL_DIGITAL]`""),
  @("connectionManagerID=`"Package.ConnectionManagers[TMP_CM_ORA]`"", "connectionManagerID=`"$($projOra):external`""),
  @("connectionManagerRefId=`"Package.ConnectionManagers[TMP_CM_ORA]`"", "connectionManagerRefId=`"Project.ConnectionManagers[CM_ORACLE_TARGET_APPS]`""),
  @("SQLTask:Connection=`"$tmpSql`"", "SQLTask:Connection=`"$projSql`""),
  @("SQLTask:Connection=`"$tmpOra`"", "SQLTask:Connection=`"$projOra`"")
)
foreach ($m in $map) { $c = ([regex]::Matches($xml, [regex]::Escape($m[0]))).Count; "{0,2} x {1}" -f $c, $m[0]; $xml = $xml.Replace($m[0], $m[1]) }
if ($xml -match 'TMP_CM_|' + [regex]::Escape($tmpSql) + '|' + [regex]::Escape($tmpOra)) { throw 'Leftover temp connection references found' }
[IO.File]::WriteAllText($OutFile, $xml, (New-Object Text.UTF8Encoding($false)))
"Final package: $OutFile ($((Get-Item $OutFile).Length) bytes)"
