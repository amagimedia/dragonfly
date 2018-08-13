package main

// create buffered channel for writing data to cloudwatch ( channel should be of non-zero capacity )
// create ticker for each rds instance
// send query command to rds instance
// wait for the query output
// send query output to cloudwatch channel

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"

	"github.com/aws/aws-sdk-go/service/cloudwatch"
	_ "github.com/go-sql-driver/mysql"
	"github.com/golang/glog"
)

type RdsConfig []struct {
	User                 string        `json:"user"`
	Endpoint             string        `json:"endpoint"`
	Password             string        `json:"password"`
	DBInstanceIdentifier string        `json:"db_instance_identifier"`
	Port                 int           `json:"port"`
	TickSeconds          time.Duration `json:"tick_seconds"`
	AwsProfile           string        `json:"aws_profile"`
	AwsRegion            string        `json:"aws_region"`
}

type metricData struct {
	MetricName     string
	MetricUnit     string
	MetricValue    float64
	DimensionName  string
	DimensionValue string
}

// Config gives list of all rds details
var Config RdsConfig
var cloudWatchChannel chan cloudwatch.PutMetricDataInput
var maxMetricDataSize int

func readConfig() {
	inputConfig := flag.String("input", "rds_config.json", "list of clouds file")
	flag.Parse()
	_, statErr := os.Stat(*inputConfig)

	if statErr != nil {
		fmt.Println(*inputConfig, " stats Error. Error: ", statErr)
		os.Exit(1)
	}
	rawData, err := ioutil.ReadFile(*inputConfig)
	if err != nil {
		fmt.Println("Failed to read ", *inputConfig)
		os.Exit(1)
	}

	rawJSON := json.RawMessage(rawData)
	jErr := json.Unmarshal(rawJSON, &Config)
	if jErr != nil {
		fmt.Println("error occured while parsing json %s", *inputConfig)
		os.Exit(1)
	}
}

// collect all query rows then create metricsInput so that if all query is less than 15 we call cloudwatch api only  once

func getConnectionPerTenant(db *sql.DB, MetricsDataList *[]metricData, DBInstanceIdentifier string) {
	results, err := db.Query("SELECT DB,COUNT(*) as count FROM INFORMATION_SCHEMA.PROCESSLIST WHERE DB IS NOT NULL GROUP BY DB ORDER BY count DESC;")
	if err != nil {
		glog.Error("Failed to get per tenant db connections for ", DBInstanceIdentifier, ". Error : ", err)
	}
	defer results.Close()
	for results.Next() {
		var dbConnection metricData
		dbConnection.DimensionValue = DBInstanceIdentifier
		dbConnection.MetricUnit = "Count"
		dbConnection.DimensionName = "ConnectionPerTenant"
		err = results.Scan(&dbConnection.MetricName, &dbConnection.MetricValue)
		if err != nil {
			glog.Error("failed to scan process list for ", DBInstanceIdentifier)
		}
		*MetricsDataList = append(*MetricsDataList, dbConnection)
	}
}

func getDbSizePerTenant(db *sql.DB, MetricsDataList *[]metricData, DBInstanceIdentifier string) {
	// Get size of each database in MB
	sizeResults, sizeErr := db.Query("SELECT table_schema 'DB Name', ROUND(SUM(data_length + index_length) / 1024 / 1024, 1) 'DB Size in MB' FROM information_schema.tables GROUP BY table_schema;")
	if sizeErr != nil {
		glog.Error("failed to get db size for ", DBInstanceIdentifier)

	}
	defer sizeResults.Close()
	for sizeResults.Next() {
		var dbSizeMetricData metricData
		dbSizeMetricData.MetricUnit = "Megabytes"
		dbSizeMetricData.DimensionName = "DatabaseSizePerTenant"
		dbSizeMetricData.DimensionValue = DBInstanceIdentifier
		err := sizeResults.Scan(&dbSizeMetricData.MetricName, &dbSizeMetricData.MetricValue)
		if err != nil {
			glog.Fatal(err)
			glog.Fatal("Failed scan for db size ", DBInstanceIdentifier)
		}
		*MetricsDataList = append(*MetricsDataList, dbSizeMetricData)
	}

}

func getNewPutMetricDataInputPointer() *cloudwatch.PutMetricDataInput {
	var metricInput cloudwatch.PutMetricDataInput
	metricInput.Namespace = aws.String("MultiTenantRds")
	return &metricInput

}

func readRdsMetrics(rds, DBInstanceIdentifier string, TickAt time.Duration) {
	db, err := sql.Open("mysql", rds)
	if err != nil {
		glog.Error("could not cmmreate sql driver ", rds)
		db.Close()
		return
	}
	pingErr := db.Ping()
	if pingErr != nil {
		glog.Error("could not connect to ", rds, " Error : ", pingErr)
		db.Close()
		return
	}
	ticker := time.NewTicker(TickAt * time.Second)
	go func() {
		for t := range ticker.C {
			fmt.Println("Tick at", t)
			var MetricsDataList []metricData
			var maxMetricsToInsert int
			getConnectionPerTenant(db, &MetricsDataList, DBInstanceIdentifier)
			getDbSizePerTenant(db, &MetricsDataList, DBInstanceIdentifier)
			var pointerToNewMetricInput = getNewPutMetricDataInputPointer()

			if len(MetricsDataList) < maxMetricDataSize {
				maxMetricsToInsert = len(MetricsDataList)
			} else {
				maxMetricsToInsert = maxMetricDataSize
			}

			//fmt.Println((*pointerToNewMetricInput).MetricData)
			//fmt.Println(maxMetricsToInsert)

			count := 0
			for _, data := range MetricsDataList {
				(*pointerToNewMetricInput).MetricData = append((*pointerToNewMetricInput).MetricData, &cloudwatch.MetricDatum{
					MetricName: aws.String(data.MetricName),
					Unit:       aws.String(data.MetricUnit),
					Value:      aws.Float64(data.MetricValue),
					Dimensions: []*cloudwatch.Dimension{
						&cloudwatch.Dimension{
							Name:  aws.String(data.DimensionName),
							Value: aws.String(data.DimensionValue),
						},
					},
				})
				count++
				if count == maxMetricsToInsert {
					cloudWatchChannel <- *pointerToNewMetricInput
					count = 0
					pointerToNewMetricInput = getNewPutMetricDataInputPointer()
				}
			}
		}
		defer db.Close()
	}()

}

func main() {
	cloudWatchChannel = make(chan cloudwatch.PutMetricDataInput, 150)
	maxMetricDataSize = 15
	rdsToAwsService := make(map[string]*cloudwatch.CloudWatch)
	readConfig()
	for _, rds := range Config {
		if rds.Port == 0 {
			rds.Port = 3306
		}

		if rds.AwsProfile == "" {
			rds.AwsProfile = "default"
		}

		if rds.AwsRegion == "" {
			rds.AwsRegion = "us-east-1"
		}

		if rds.TickSeconds == 0 {
			rds.TickSeconds = 300 // 5 minutes
		}

		rdsConnectionString := fmt.Sprintf("%s:%s@tcp(%s:%d)/", rds.User, rds.Password, rds.Endpoint, rds.Port)
		readRdsMetrics(rdsConnectionString, rds.DBInstanceIdentifier, rds.TickSeconds)

		sess := cloudwatch.New(session.Must(session.NewSessionWithOptions(session.Options{
			SharedConfigState: session.SharedConfigEnable,
			Config:            aws.Config{Region: aws.String(rds.AwsRegion)},
		})))
		rdsToAwsService[rds.DBInstanceIdentifier] = sess
	}

	for i := range cloudWatchChannel {
		_, err := rdsToAwsService[(*(i.MetricData[0].Dimensions[0].Value))].PutMetricData(&i)
		if err != nil {
			glog.Error(err.Error())
		}
	}
}
