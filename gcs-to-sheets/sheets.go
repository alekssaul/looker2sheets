package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	sheets "google.golang.org/api/sheets/v4"
)

func UpdateSheets(obj string, data [][]string) (err error) {
	// Parse object to determine the flat objectname without csv
	// i.e: daily_hours-1674409641.csv
	o := strings.Split(obj, "/")
	objectname := o[len(o)-1]                   // daily_hours-1674409641.csv
	d := strings.TrimSuffix(objectname, ".csv") // daily_hours-1674409641
	ds := strings.Split(d, "-")
	dashboardname := ds[0] // daily_hours

	ctx := context.Background()
	sheetsService, err := sheets.NewService(ctx)
	if err != nil {
		return fmt.Errorf("could not connect to sheets API : %v", err)
	}

	spreadsheetId := os.Getenv("spreadsheet_id")

	readRange := dashboardname + "!A1:Z3" // This will retrieve the values of first 3 rows in the sheet

	// Use the service to retrieve the values
	response, err := sheetsService.Spreadsheets.Values.Get(spreadsheetId, readRange).Do()
	if err != nil {
		return fmt.Errorf("%s : unable to retrieve values: %v", dashboardname, err)
	}

	sheetempty := false
	if len(response.Values) == 0 {
		log.Printf("%s Sheet is empty", dashboardname)
		sheetempty = true
	}

	// Itirate over existing sheet
	// Find which row first date entry and its rownumber
	var rownumber int
	var firstdate string
	topdate := false
	for i, row := range response.Values {
		for _, cell := range row {
			if (isDate(fmt.Sprintf("%s", cell)) || isMonth(fmt.Sprintf("%s", cell)) ||
				isValidDateTimeString(fmt.Sprintf("%s", cell))) && !topdate {
				rownumber = i
				firstdate = fmt.Sprintf("%s", cell)
				topdate = true
				log.Printf("%s : Top Date on Existing spreadsheet is %s", dashboardname, cell)
			}
		}
	}

	writeRange := dashboardname + "!A1"
	if !sheetempty {
		writeRange = dashboardname + "!A" + fmt.Sprintf("%v", rownumber+1)
	}

	// Itirate over data to append
	topdate = false
	var vr sheets.ValueRange
	for c, row := range data {
		var interfaces []interface{}
		for i, s := range row {
			if !topdate && (isDate(s) || isMonth(s) || isValidDateTimeString(s)) && !sheetempty {
				topdate = true
				// if the top date we are inserting is different than the top date of the sheet
				// we insert a new empty row first
				if s != firstdate {
					log.Printf("%s : New data found", dashboardname)
					insertRow(int64(rownumber+1), spreadsheetId, dashboardname, sheetsService, true)
				}
			}

			// skip headers
			if sheetempty || isDate(row[0]) || isMonth(row[0]) || isValidDateTimeString(row[0]) {
				s = strings.TrimLeft(s, "\"")
				s = strings.TrimRight(s, "\"")
				s = strings.Replace(s, ",", "", -1) // data source have numbers with comma in between

				if isValidDateTimeString(s) {
					// remove the datetime stamp
					interfaces = append(interfaces, strings.TrimRight(s, " 0:00:00"))
				} else {
					interfaces = append(interfaces, s)
				}
			}

			// if last column length is 4, add empty cell and SUM
			if i == len(row)-2 && len(row) == 4 && isDate(row[0]) {
				interfaces = append(interfaces, "")
			}

			// if last column add a SUM cell
			if i == len(row)-1 && (len(row) == 5 || len(row) == 4) && isDate(row[0]) {
				interfaces = append(interfaces, fmt.Sprintf("=SUM(B%v:E%v)", c+1, c+1))
			}

		}
		vr.Values = append(vr.Values, interfaces)

	}

	log.Printf("%s : Append data: %v", dashboardname, vr.Values)

	_, err = sheetsService.Spreadsheets.Values.Update(spreadsheetId, writeRange, &vr).ValueInputOption("USER_ENTERED").Do()
	if err != nil {
		return fmt.Errorf("%s : unable to update data in the sheet. %v", dashboardname, err)
	}

	// Randomly selected a dashboard name that executes once to update the summary
	if dashboardname == "svod_subscriptions_kpi" {
		err = updateSummary(spreadsheetId, sheetsService)
		if err != nil {
			return fmt.Errorf("%s : Could not update the Summary sheet. %v", dashboardname, err)
		}
	}

	return nil
}

func isDate(date string) (b bool) {
	layout := "2006-01-02"
	_, err := time.Parse(layout, date)
	if err != nil {
		return false
	} else {
		return true
	}
}

func isMonth(month string) (b bool) {
	layout := "2006-01"
	_, err := time.Parse(layout, month)
	if err != nil {
		return false
	} else {
		return true
	}

}

func isValidDateTimeString(str string) bool {
	_, err := time.Parse("2006-01-02 15:04:05", str)
	return err == nil
}

func insertRow(insertionIndex int64, spreadsheetID string, sheetName string, s *sheets.Service, inheritance bool) (err error) {
	// Get the spreadsheet
	resp, err := s.Spreadsheets.Get(spreadsheetID).Do()
	if err != nil {
		return fmt.Errorf("unable to get spreadsheet: %v", err)
	}

	var sheetId int64
	sheetId = 0
	for _, sheet := range resp.Sheets {
		if sheet.Properties.Title == sheetName {
			// Print the sheet ID
			sheetId = sheet.Properties.SheetId
			log.Printf("Sheet %s ID: %v", sheetName, sheet.Properties.SheetId)
			break
		}
	}

	// Create the request body
	request := &sheets.Request{
		InsertDimension: &sheets.InsertDimensionRequest{
			Range: &sheets.DimensionRange{
				SheetId:    sheetId,
				Dimension:  "ROWS",
				StartIndex: insertionIndex - 1,
				EndIndex:   insertionIndex,
			},
			InheritFromBefore: inheritance,
		},
	}
	batchUpdateRequest := &sheets.BatchUpdateSpreadsheetRequest{
		Requests: []*sheets.Request{request},
	}
	// Use the service to insert the row
	_, err = s.Spreadsheets.BatchUpdate(spreadsheetID, batchUpdateRequest).Do()
	if err != nil {
		return fmt.Errorf("unable to insert row: %v", err)
	}

	log.Printf("Empty row inserted to row: %v on sheet %s", insertionIndex, sheetName)
	return nil
}

func updateSummary(spreadsheetId string, sheetsService *sheets.Service) (err error) {
	var vr sheets.ValueRange

	var interfaces []interface{}
	interfaces = append(interfaces, getYesterdayDate())

	cell := "=VLOOKUP(A2,daily_hours!A:F,6,FALSE)"
	interfaces = append(interfaces, cell)

	cell = "=VLOOKUP(A2,daily_users!A:F,6,FALSE)"
	interfaces = append(interfaces, cell)

	cell = "=VLOOKUP(A2,svod_daily_users!A:F,6,FALSE)"
	interfaces = append(interfaces, cell)

	cell = "=VLOOKUP(A2,svod_daily_hours!A:F,6,FALSE)"
	interfaces = append(interfaces, cell)

	cell = "=SUM(svod_subscriptions_kpi!B3:D3)"
	interfaces = append(interfaces, cell)

	cell = "=F2-H2"
	interfaces = append(interfaces, cell)

	cell = "=svod_users_us!I2+svod_users_mx!I2+svod_users_rolac!I2"
	interfaces = append(interfaces, cell)

	cell = "=E2/D2"
	interfaces = append(interfaces, cell)

	cell = "=(B2-E2)/(C2-D2)"
	interfaces = append(interfaces, cell)

	vr.Values = append(vr.Values, interfaces)
	log.Printf("Summary : Append data: %v", vr.Values)

	insertRow(int64(2), spreadsheetId, "Summary", sheetsService, false)

	firstRow := "Summary" + "!A2"
	_, err = sheetsService.Spreadsheets.Values.Update(spreadsheetId, firstRow, &vr).ValueInputOption("USER_ENTERED").Do()
	if err != nil {
		return fmt.Errorf("summary : unable to update data. %v", err)
	}

	log.Printf("Updated the Summary Sheet")
	return nil
}

func getYesterdayDate() string {
	// Get the current time in the UTC timezone
	now := time.Now().UTC()

	// Subtract 1 day from the current time
	yesterday := now.AddDate(0, 0, -1)

	// Format the yesterday date as a string in "YYYY-MM-DD" format
	yesterdayStr := yesterday.Format("2006-01-02")

	return yesterdayStr
}
