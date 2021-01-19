param(
    [parameter(Mandatory=$true)]
    [ValidateNotNull()]
    $objectNames,
    [parameter(Mandatory=$true)]
    [ValidateNotNull()]
    $outputBasePath
)

if ($objectNames -isnot [system.array]) {
    echo "Did not provided an array of values. Use -objectNames = @('val1', 'val2')."
    exit 1;
}

. ..\Utilities\DBA_Utils.ps1

$accts = Get-AccountDBConfiguration


$objectNames | % {
    
    $objectName = $_


    $path = "{0}\{2}_{1}" -f $outputBasePath, $objectName, (Get-Date -format "yyyyMMdd")
    if (Test-Path $path) {
        echo ("Path Exists: {0}. Please remove path to run a comparison." -f $path)
        exit 1;
    } else {
        echo ("Creating path: {0}. Diff information stored in the folder." -f $path)
        New-Item $path -type directory | Out-Null
    }

    $allDefs = @()

    #set variables for all queries
    $vars = "objName = '$objectName'"

    $accts | % {
        $accountId = $_.accountID
        #echo ("Account_ID: {0} Server: {1} Database: {2}" -f $accountId, $_.server, $_.database)



        $query = "

        declare @val varchar(100) = 'MISSING'

        select
	        @val  = convert(varchar(100), HASHBYTES('SHA1', left(sm.definition, 4000)), 2)
        from sys.sql_modules sm
        inner join sys.objects o
	        on sm.object_id = o.object_id
        where
	        o.name = `$(objName)

        select @val as def
        "

        Push-Location

        $query_results = Invoke-Sqlcmd -Query $query -ServerInstance $_.server -Database $_.database -Variable $vars

        Pop-Location

        $objDef = ""
        foreach ($row in $query_results) {
            $objDef = $row.def
        }
        $allDefs += @{accountID=$accountId;def=$objDef}
    }

    $defVersions = @{}

    $allDefs | % {
        $def = $_.def
        $acct = $_.accountID

        if ($defVersions.ContainsKey($def)) {
            $defVersions[$def] += $acct
        } else {
            $defVersions[$def] = @($acct)
        }
    }

    $defVersions.GetEnumerator() | % {
        $path = "{0}\{2}_{1}" -f $outputBasePath, $objectName, (Get-Date -format "yyyyMMdd")
        $versionPath = "{0}\{1}.clients.txt" -f $path, $_.Key
        #New-Item $versionPath -type directory | Out-Null
        $_.Value | % {
            echo $_ >> $versionPath
        }
    }

}