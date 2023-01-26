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
		return fmt.Errorf("unable to retrieve values: %v", err)
	}

	sheetempty := false
	if len(response.Values) == 0 {
		log.Printf("Sheet %s is empty", dashboardname)
		sheetempty = true
	}

	// Itirate over existing sheet
	// Find which row first date entry and its rownumber
	var rownumber int
	var firstdate string
	topdate := false
	for i, row := range response.Values {
		for _, cell := range row {
			if (isDate(fmt.Sprintf("%s", cell)) || isMonth(fmt.Sprintf("%s", cell))) && !topdate {
				rownumber = i
				firstdate = fmt.Sprintf("%s", cell)
				topdate = true
				log.Printf("Top Date on Existing spreadsheet is %s", cell)
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
	for _, row := range data {
		var interfaces []interface{}
		for _, s := range row {
			if !topdate && (isDate(s) || isMonth(s)) && !sheetempty {
				topdate = true
				// if the top date we are inserting is different than the top date of the sheet
				// we insert a new empty row first
				if s != firstdate {
					log.Printf("New data found for : %s", dashboardname)
					insertRow(int64(rownumber+1), spreadsheetId, dashboardname, sheetsService)
				}
			}
			// skip headers
			if sheetempty || isDate(row[0]) || isMonth(row[0]) {
				s = strings.TrimLeft(s, "\"")
				s = strings.TrimRight(s, "\"")
				s = strings.Replace(s, ",", "", -1) // data source have numbers with comma in between
				interfaces = append(interfaces, s)
			}
		}
		vr.Values = append(vr.Values, interfaces)

	}

	log.Printf("Append data: %v", vr.Values)
	resp, err := sheetsService.Spreadsheets.Values.Update(spreadsheetId, writeRange, &vr).ValueInputOption("RAW").Do()
	if err != nil {
		return fmt.Errorf("unable to retrieve data from sheet. %v", err)
	}

	log.Printf("Response : %v", resp)

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

func insertRow(insertionIndex int64, spreadsheetID string, sheetName string, s *sheets.Service) (err error) {
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
			InheritFromBefore: true,
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
