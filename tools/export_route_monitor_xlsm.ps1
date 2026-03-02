param(
    [Parameter(Mandatory = $true)]
    [string]$InputXlsx,
    [string]$OutputXlsm = ""
)

$ErrorActionPreference = "Stop"

function Resolve-FullPath([string]$PathValue) {
    if ([System.IO.Path]::IsPathRooted($PathValue)) {
        return [System.IO.Path]::GetFullPath($PathValue)
    }
    return [System.IO.Path]::GetFullPath((Join-Path (Get-Location) $PathValue))
}

$inPath = Resolve-FullPath $InputXlsx
if (-not (Test-Path -LiteralPath $inPath)) {
    throw "Input workbook not found: $inPath"
}

if ([string]::IsNullOrWhiteSpace($OutputXlsm)) {
    $OutputXlsm = [System.IO.Path]::ChangeExtension($inPath, ".xlsm")
}
$outPath = Resolve-FullPath $OutputXlsm

$excel = $null
$wb = $null
try {
    $excel = New-Object -ComObject Excel.Application
    $excel.Visible = $false
    $excel.DisplayAlerts = $false

    $wb = $excel.Workbooks.Open($inPath)
    # 52 = xlOpenXMLWorkbookMacroEnabled
    $wb.SaveAs($outPath, 52)

    $wsData = $null
    try {
        $wsData = $wb.Worksheets.Item("Route Filter View")
    } catch {
        throw "Worksheet 'Route Filter View' not found. Generate latest route monitor workbook first."
    }
    $wsMain = $null
    try {
        $wsMain = $wb.Worksheets.Item("Route Flight Fare Monitor")
    } catch {
        throw "Worksheet 'Route Flight Fare Monitor' not found. Generate latest route monitor workbook first."
    }
    $wsBlockIndex = $null
    try {
        $wsBlockIndex = $wb.Worksheets.Item("Route Block Index")
    } catch {
        throw "Worksheet 'Route Block Index' not found. Regenerate route monitor with latest output writer changes."
    }

    $wsCtl = $null
    try {
        $wsCtl = $wb.Worksheets.Item("Macro Control")
    } catch {
        $wsCtl = $wb.Worksheets.Add()
        $wsCtl.Name = "Macro Control"
    }

    $wsCtl.Cells.Item(1, 1).Value2 = "Route Monitor Macro Controls"
    $wsCtl.Cells.Item(2, 1).Value2 = "Airlines CSV (optional)"
    $wsCtl.Cells.Item(2, 2).Value2 = ""
    $wsCtl.Cells.Item(3, 1).Value2 = "Signals CSV (optional)"
    $wsCtl.Cells.Item(3, 2).Value2 = ""
    $wsCtl.Cells.Item(5, 1).Value2 = "Main sheet is click-based. Use CSV fields only for Route Filter View."
    $wsCtl.Cells.Item(6, 1).Value2 = "ApplyRouteFilters"
    $wsCtl.Cells.Item(7, 1).Value2 = "ClearRouteFilters"
    $wsCtl.Columns.Item("A:B").AutoFit() | Out-Null

    $vba = @"
Option Explicit

Private Function ParseCsv(ByVal raw As String) As Variant
    Dim txt As String
    txt = Trim(UCase(raw))
    If Len(txt) = 0 Then
        ParseCsv = Empty
        Exit Function
    End If
    Dim arr0() As String
    arr0 = Split(txt, ",")
    Dim tmp() As String
    ReDim tmp(0 To UBound(arr0))
    Dim i As Long, n As Long
    n = -1
    For i = LBound(arr0) To UBound(arr0)
        Dim v As String
        v = Trim(arr0(i))
        If Len(v) > 0 Then
            n = n + 1
            tmp(n) = v
        End If
    Next i
    If n < 0 Then
        ParseCsv = Empty
        Exit Function
    End If
    ReDim Preserve tmp(0 To n)
    ParseCsv = tmp
End Function

Private Function FindHeaderColumn(ByVal ws As Worksheet, ByVal headerRow As Long, ByVal headerName As String) As Long
    Dim lastCol As Long
    lastCol = ws.Cells(headerRow, ws.Columns.Count).End(xlToLeft).Column
    Dim c As Long
    For c = 1 To lastCol
        If UCase(Trim(CStr(ws.Cells(headerRow, c).Value2))) = UCase(Trim(headerName)) Then
            FindHeaderColumn = c
            Exit Function
        End If
    Next c
    FindHeaderColumn = 0
End Function

Private Sub AddUnique(ByVal coll As Collection, ByVal token As String)
    If Len(token) = 0 Then Exit Sub
    On Error Resume Next
    coll.Add token, token
    On Error GoTo 0
End Sub

Private Function CsvToCollection(ByVal raw As String) As Collection
    Dim out As New Collection
    Dim arr As Variant
    arr = ParseCsv(raw)
    If IsEmpty(arr) Then
        Set CsvToCollection = out
        Exit Function
    End If
    Dim i As Long
    For i = LBound(arr) To UBound(arr)
        AddUnique out, CStr(arr(i))
    Next i
    Set CsvToCollection = out
End Function

Private Function CollectionToCsv(ByVal coll As Collection) As String
    Dim txt As String
    Dim item As Variant
    For Each item In coll
        If Len(txt) > 0 Then txt = txt & ","
        txt = txt & UCase(CStr(item))
    Next item
    CollectionToCsv = txt
End Function

Private Function CloneCollection(ByVal src As Collection) As Collection
    Dim out As New Collection
    Dim item As Variant
    For Each item In src
        AddUnique out, UCase(CStr(item))
    Next item
    Set CloneCollection = out
End Function

Private Function CollectionContains(ByVal coll As Collection, ByVal token As String) As Boolean
    If coll Is Nothing Then Exit Function
    Dim item As Variant
    For Each item In coll
        If UCase(CStr(item)) = UCase(token) Then
            CollectionContains = True
            Exit Function
        End If
    Next item
End Function

Private Function CollectionEquals(ByVal a As Collection, ByVal b As Collection) As Boolean
    If a Is Nothing Or b Is Nothing Then Exit Function
    If a.Count <> b.Count Then Exit Function
    Dim item As Variant
    For Each item In a
        If Not CollectionContains(b, UCase(CStr(item))) Then Exit Function
    Next item
    CollectionEquals = True
End Function

Private Function CsvIntersectsSelection(ByVal csvText As String, ByVal selected As Collection) As Boolean
    If selected Is Nothing Or selected.Count = 0 Then
        CsvIntersectsSelection = True
        Exit Function
    End If
    Dim arr() As String
    arr = Split(UCase(CStr(csvText)), ",")
    Dim i As Long, token As String
    For i = LBound(arr) To UBound(arr)
        token = Trim(arr(i))
        If Len(token) > 0 Then
            If CollectionContains(selected, token) Then
                CsvIntersectsSelection = True
                Exit Function
            End If
        End If
    Next i
    CsvIntersectsSelection = False
End Function

Private Function NormalizeSignalToken(ByVal raw As String) As String
    Dim t As String
    t = UCase(Trim(raw))
    If Len(t) = 0 Then Exit Function
    If InStr(t, "INCREASE") > 0 Then
        NormalizeSignalToken = "INCREASE"
        Exit Function
    End If
    If InStr(t, "DECREASE") > 0 Then
        NormalizeSignalToken = "DECREASE"
        Exit Function
    End If
    If t = "NEW" Then
        NormalizeSignalToken = "NEW"
        Exit Function
    End If
    If InStr(t, "SOLD") > 0 Then
        NormalizeSignalToken = "SOLD OUT"
        Exit Function
    End If
    If InStr(t, "UNKNOWN") > 0 Then
        NormalizeSignalToken = "UNKNOWN"
        Exit Function
    End If
End Function

Private Function GetLegendAirlines(ByVal wsMain As Worksheet) As Collection
    Dim out As New Collection
    Dim c As Long
    For c = 2 To 250
        Dim v As String
        v = UCase(Trim(CStr(wsMain.Cells(2, c).Value2)))
        If Len(v) = 0 Then Exit For
        AddUnique out, v
    Next c
    Set GetLegendAirlines = out
End Function

Private Function GetLegendSignals(ByVal wsMain As Worksheet) As Collection
    Dim out As New Collection
    Dim c As Long
    For c = 2 To 250
        Dim t As String
        t = NormalizeSignalToken(CStr(wsMain.Cells(3, c).Value2))
        If Len(t) = 0 Then Exit For
        AddUnique out, t
    Next c
    If out.Count = 0 Then
        AddUnique out, "INCREASE"
        AddUnique out, "DECREASE"
        AddUnique out, "NEW"
        AddUnique out, "SOLD OUT"
        AddUnique out, "UNKNOWN"
    End If
    Set GetLegendSignals = out
End Function

Private Function StateCell(ByVal kind As String) As String
    If LCase(kind) = "air" Then
        StateCell = "B2"
    Else
        StateCell = "B3"
    End If
End Function

Private Function GetUniverse(ByVal kind As String, ByVal wsMain As Worksheet) As Collection
    If LCase(kind) = "air" Then
        Set GetUniverse = GetLegendAirlines(wsMain)
    Else
        Set GetUniverse = GetLegendSignals(wsMain)
    End If
End Function

Private Function GetSelected(ByVal kind As String, ByVal wsCtl As Worksheet, ByVal wsMain As Worksheet) As Collection
    Dim allVals As Collection
    Set allVals = GetUniverse(kind, wsMain)

    Dim raw As String
    raw = CStr(wsCtl.Range(StateCell(kind)).Value2)
    Dim parsed As Collection
    Set parsed = CsvToCollection(raw)

    If parsed.Count = 0 Then
        Set GetSelected = allVals
        Exit Function
    End If

    Dim out As New Collection
    Dim item As Variant
    For Each item In parsed
        If CollectionContains(allVals, UCase(CStr(item))) Then
            AddUnique out, UCase(CStr(item))
        End If
    Next item

    If out.Count = 0 Then
        Set out = allVals
    End If
    Set GetSelected = out
End Function

Private Sub SetSelected(ByVal kind As String, ByVal wsCtl As Worksheet, ByVal coll As Collection)
    wsCtl.Range(StateCell(kind)).Value2 = CollectionToCsv(coll)
End Sub

Private Sub ToggleSelection(ByVal kind As String, ByVal token As String)
    Dim wsCtl As Worksheet, wsMain As Worksheet
    Set wsCtl = ThisWorkbook.Worksheets("Macro Control")
    Set wsMain = ThisWorkbook.Worksheets("Route Flight Fare Monitor")

    token = UCase(Trim(token))
    If Len(token) = 0 Then Exit Sub

    Dim sel As Collection, allVals As Collection
    Set sel = GetSelected(kind, wsCtl, wsMain)
    Set allVals = GetUniverse(kind, wsMain)

    Dim nextVals As Collection
    Set nextVals = New Collection

    If CollectionEquals(sel, allVals) Then
        AddUnique nextVals, token
    ElseIf sel.Count = 1 And CollectionContains(sel, token) Then
        Set nextVals = CloneCollection(allVals)
    Else
        Set nextVals = CloneCollection(sel)
        If CollectionContains(nextVals, token) Then
            On Error Resume Next
            nextVals.Remove token
            On Error GoTo 0
        Else
            AddUnique nextVals, token
        End If
        If nextVals.Count = 0 Then
            Set nextVals = CloneCollection(allVals)
        End If
    End If

    SetSelected kind, wsCtl, nextVals
End Sub

Private Sub RefreshLegendSelectionStyling()
    Dim wsCtl As Worksheet, wsMain As Worksheet
    Set wsCtl = ThisWorkbook.Worksheets("Macro Control")
    Set wsMain = ThisWorkbook.Worksheets("Route Flight Fare Monitor")

    Dim selAir As Collection, selSig As Collection
    Set selAir = GetSelected("air", wsCtl, wsMain)
    Set selSig = GetSelected("sig", wsCtl, wsMain)

    Dim c As Long
    For c = 2 To 250
        Dim at As String
        at = UCase(Trim(CStr(wsMain.Cells(2, c).Value2)))
        If Len(at) = 0 Then Exit For
        wsMain.Cells(2, c).Font.Strikethrough = Not CollectionContains(selAir, at)
    Next c

    For c = 2 To 250
        Dim st As String
        st = NormalizeSignalToken(CStr(wsMain.Cells(3, c).Value2))
        If Len(st) = 0 Then Exit For
        wsMain.Cells(3, c).Font.Strikethrough = Not CollectionContains(selSig, st)
    Next c
End Sub

Public Sub ApplyRouteFilters()
    Dim wsData As Worksheet, wsCtl As Worksheet
    Set wsData = ThisWorkbook.Worksheets("Route Filter View")
    Set wsCtl = ThisWorkbook.Worksheets("Macro Control")

    Dim headerRow As Long
    headerRow = 7

    Dim lastRow As Long, lastCol As Long
    lastRow = wsData.Cells(wsData.Rows.Count, 1).End(xlUp).Row
    lastCol = wsData.Cells(headerRow, wsData.Columns.Count).End(xlToLeft).Column
    If lastRow <= headerRow Then Exit Sub

    Dim rng As Range
    Set rng = wsData.Range(wsData.Cells(headerRow, 1), wsData.Cells(lastRow, lastCol))

    If wsData.AutoFilterMode Then wsData.AutoFilterMode = False
    rng.AutoFilter

    Dim airlineCol As Long, signalCol As Long
    airlineCol = FindHeaderColumn(wsData, headerRow, "airline")
    signalCol = FindHeaderColumn(wsData, headerRow, "signal_primary")

    Dim arr As Variant
    arr = ParseCsv(CStr(wsCtl.Range("B2").Value2))
    If Not IsEmpty(arr) And airlineCol > 0 Then
        rng.AutoFilter Field:=airlineCol, Criteria1:=arr, Operator:=xlFilterValues
    End If

    arr = ParseCsv(CStr(wsCtl.Range("B3").Value2))
    If Not IsEmpty(arr) And signalCol > 0 Then
        rng.AutoFilter Field:=signalCol, Criteria1:=arr, Operator:=xlFilterValues
    End If

    wsData.Activate
End Sub

Public Sub ClearRouteFilters()
    Dim wsData As Worksheet
    Set wsData = ThisWorkbook.Worksheets("Route Filter View")
    If wsData.FilterMode Then wsData.ShowAllData
    wsData.AutoFilterMode = False
    wsData.Activate
End Sub

Public Sub ApplyMainSheetFilters()
    On Error GoTo EH

    Dim wsCtl As Worksheet, wsMain As Worksheet, wsIdx As Worksheet
    Set wsCtl = ThisWorkbook.Worksheets("Macro Control")
    Set wsMain = ThisWorkbook.Worksheets("Route Flight Fare Monitor")
    Set wsIdx = ThisWorkbook.Worksheets("Route Block Index")

    Dim selAir As Collection, selSig As Collection
    Set selAir = GetSelected("air", wsCtl, wsMain)
    Set selSig = GetSelected("sig", wsCtl, wsMain)

    wsMain.Rows.Hidden = False
    wsMain.Rows("1:4").Hidden = False

    Dim lastRow As Long
    lastRow = wsIdx.Cells(wsIdx.Rows.Count, 1).End(xlUp).Row
    Dim r As Long
    For r = 2 To lastRow
        Dim startRow As Long, endRow As Long
        startRow = CLng(Val(wsIdx.Cells(r, 2).Value2))
        endRow = CLng(Val(wsIdx.Cells(r, 3).Value2))
        If startRow <= 0 Or endRow < startRow Then GoTo NextRow

        Dim airlinesCsv As String, signalsCsv As String
        airlinesCsv = CStr(wsIdx.Cells(r, 4).Value2)
        signalsCsv = CStr(wsIdx.Cells(r, 5).Value2)

        Dim keepBlock As Boolean
        keepBlock = CsvIntersectsSelection(airlinesCsv, selAir) And CsvIntersectsSelection(signalsCsv, selSig)
        wsMain.Rows(CStr(startRow) & ":" & CStr(endRow)).Hidden = Not keepBlock
NextRow:
    Next r

    RefreshLegendSelectionStyling
    wsMain.Activate
    Exit Sub
EH:
    MsgBox "ApplyMainSheetFilters failed: " & Err.Description, vbExclamation
End Sub

Public Sub ClearMainSheetFilters()
    Dim wsCtl As Worksheet
    Set wsCtl = ThisWorkbook.Worksheets("Macro Control")
    wsCtl.Range("B2").Value2 = ""
    wsCtl.Range("B3").Value2 = ""
    ApplyMainSheetFilters
End Sub

Public Sub HandleLegendClick(ByVal ws As Worksheet, ByVal Target As Range)
    If ws Is Nothing Or Target Is Nothing Then Exit Sub
    If ws.Name <> "Route Flight Fare Monitor" Then Exit Sub
    If Target.CountLarge <> 1 Then Exit Sub

    Dim r As Long, c As Long
    r = Target.Row
    c = Target.Column

    If r = 2 Then
        If c = 1 Then
            ThisWorkbook.Worksheets("Macro Control").Range("B2").Value2 = ""
            ApplyMainSheetFilters
            Exit Sub
        End If
        Dim airToken As String
        airToken = UCase(Trim(CStr(Target.Value2)))
        If Len(airToken) = 0 Then Exit Sub
        If CollectionContains(GetLegendAirlines(ws), airToken) Then
            ToggleSelection "air", airToken
            ApplyMainSheetFilters
        End If
        Exit Sub
    End If

    If r = 3 Then
        If c = 1 Then
            ThisWorkbook.Worksheets("Macro Control").Range("B3").Value2 = ""
            ApplyMainSheetFilters
            Exit Sub
        End If
        Dim sigToken As String
        sigToken = NormalizeSignalToken(CStr(Target.Value2))
        If Len(sigToken) = 0 Then Exit Sub
        If CollectionContains(GetLegendSignals(ws), sigToken) Then
            ToggleSelection "sig", sigToken
            ApplyMainSheetFilters
        End If
        Exit Sub
    End If
End Sub
"@

    try {
        $vbProj = $wb.VBProject
        foreach ($comp in @($vbProj.VBComponents)) {
            if ($comp.Name -eq "RouteMonitorFilters") {
                $vbProj.VBComponents.Remove($comp)
                break
            }
        }
        # 1 = vbext_ct_StdModule
        $vbComp = $vbProj.VBComponents.Add(1)
        $vbComp.Name = "RouteMonitorFilters"
        $vbComp.CodeModule.AddFromString($vba) | Out-Null

        $wsComp = $vbProj.VBComponents.Item($wsMain.CodeName)
        $wsCode = @"
Option Explicit

Private Sub Worksheet_SelectionChange(ByVal Target As Range)
    On Error Resume Next
    RouteMonitorFilters.HandleLegendClick Me, Target
End Sub
"@
        $lineCount = $wsComp.CodeModule.CountOfLines
        if ($lineCount -gt 0) {
            $wsComp.CodeModule.DeleteLines(1, $lineCount)
        }
        $wsComp.CodeModule.AddFromString($wsCode) | Out-Null
    } catch {
        throw "VBA injection failed. Enable Excel setting: Trust Center > Macro Settings > Trust access to the VBA project object model."
    }

    foreach ($shape in @($wsCtl.Shapes)) {
        if ($shape.Name -eq "btnApplyFilters" -or $shape.Name -eq "btnClearFilters") {
            $shape.Delete()
        }
    }

    $btn1 = $wsCtl.Shapes.AddShape(1, 20, 150, 170, 28)
    $btn1.Name = "btnApplyFilters"
    $btn1.TextFrame.Characters().Text = "Apply Route Filters"
    $btn1.OnAction = "ApplyRouteFilters"

    $btn2 = $wsCtl.Shapes.AddShape(1, 210, 150, 170, 28)
    $btn2.Name = "btnClearFilters"
    $btn2.TextFrame.Characters().Text = "Clear Route Filters"
    $btn2.OnAction = "ClearRouteFilters"

    # In-sheet click-action controls on current monitor tab.
    $shapeNamesMain = @()
    foreach ($shape in @($wsMain.Shapes)) {
        $n = [string]$shape.Name
        if (
            $n -eq "btnClearMainFilters" -or
            $n -like "mflt_air_*" -or
            $n -like "mflt_sig_*"
        ) {
            $shapeNamesMain += $n
        }
    }
    foreach ($n in $shapeNamesMain) {
        try { $wsMain.Shapes.Item($n).Delete() } catch {}
    }

    foreach ($cb in @($wsMain.CheckBoxes())) {
        $n = [string]$cb.Name
        if ($n -like "mflt_air_*" -or $n -like "mflt_sig_*") {
            try { $cb.Delete() } catch {}
        }
    }

    $anchorCol = 28
    $baseLeft = [double]$wsMain.Cells.Item(1, $anchorCol).Left
    $baseTop = [double]$wsMain.Cells.Item(1, $anchorCol).Top
    $wsMain.Cells.Item(1, $anchorCol).Value2 = "Interactive Actions (Click Legends)"
    $wsMain.Cells.Item(2, $anchorCol).Value2 = "Click Airline cells (row 2) and Signal cells (row 3)."
    $wsMain.Cells.Item(3, $anchorCol).Value2 = "Click 'Airlines' or 'Signals' label to reset that group."

    $btnMainB = $wsMain.Shapes.AddShape(1, $baseLeft, $baseTop + 62, 200, 24)
    $btnMainB.Name = "btnClearMainFilters"
    $btnMainB.TextFrame.Characters().Text = "Clear Main Sheet Filters"
    $btnMainB.OnAction = "ClearMainSheetFilters"

    try { $excel.Run("ApplyMainSheetFilters") | Out-Null } catch {}

    $wb.Save()
    Write-Output "xlsm_exported=$outPath"
} finally {
    if ($wb -ne $null) { $wb.Close($true) | Out-Null }
    if ($excel -ne $null) {
        $excel.Quit() | Out-Null
        [System.Runtime.InteropServices.Marshal]::ReleaseComObject($excel) | Out-Null
    }
}
