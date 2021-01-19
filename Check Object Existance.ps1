param(
    $objectName
)

. ..\Utilities\DBA_Utils.ps1

$accts = Get-AccountDBConfiguration

$accountExistance = @()

#set variables for all queries
$vars = "objName = '$objectName'"

$accts | % {
    $accountId = $_.accountID
    #echo ("Account_ID: {0} Server: {1} Database: {2}" -f $accountId, $_.server, $_.database)



    $query = "

    if exists
    (
        select 1 from sys.objects
        where
            name = `$(objName)
    )
        select cast(1 as bit) as ObjectExists
    else
        select cast(0 as bit) as ObjectExists
    "

    Push-Location

    $query_results = Invoke-Sqlcmd -Query $query -ServerInstance $_.server -Database $_.database -Variable $vars

    Pop-Location

    $objectExists = 0
    foreach ($row in $query_results) {
        $objectExists = $row.ObjectExists
    }
    $accountExistance += @{accountID=$accountId;objExists=$objectExists}
}


$accountExistance | % {
    if ($_.objExists -ieq "true") {
        echo ("{0} object status: {1}" -f $_.accountID, $_.objExists)
    }
}