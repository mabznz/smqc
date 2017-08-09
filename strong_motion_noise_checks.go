/*
Strong Motion Noise Check

Queries hazard database for strong motion values and orders by most likely station
to be non performing with data quality issues.

Please refer for examples
https://wiki.geonet.org.nz/display/dmcops/Strong+Motion+Noise+checks

As hazard database only stores summarised pga, pgv and mmi data values ranging for an
hour, script should be set up to run every hour. It will create files
for each noise check if they do not exist and continually append to these files once they
do exist. This is to allow data to be collected for longer periods as data quality poor performance
may be related to a regular weeekly factor for instance.

Dependancies:
For script to run hazard_r user password must be an environment variable
HAZARD_PASSWD

Also needs to run in Geonet VPN.

Logs and writes data files to /tmp. Change to appropiate.

*/

package main

import (
        "database/sql"
        "fmt"
        "os"
        _ "github.com/lib/pq"
        "log"
        "path/filepath"
)

const (
        noiseCountSQL = `
SELECT
        CURRENT_TIMESTAMP,
        loc.station,
        loc.blacklist,
        'pga-' || pga.vertical AS vertical,
        count(pga.*) AS noise_count
FROM
	impact.pga pga
	RIGHT OUTER JOIN impact.source loc ON loc.sourcepk = pga.sourcepk
GROUP BY
	loc.station, loc.blacklist, 'pga-' || pga.vertical
HAVING count(pga.*) > 16
UNION
SELECT
        CURRENT_TIMESTAMP,
	loc.station,
        loc.blacklist,
        'pgv-' || pgv.vertical,
        count(pgv.*)
FROM
	impact.pgv pgv
	RIGHT OUTER JOIN impact.source loc ON loc.sourcepk = pgv.sourcepk
GROUP BY
	loc.station, loc.blacklist, 'pgv-' || pgv.vertical
ORDER BY noise_count desc
        LIMIT 10`

    ratioDiffSQL = `
SELECT
        CURRENT_TIMESTAMP,
        loc.station,
        loc.blacklist,
	CASE WHEN max_vert.max_pga > max_hori.max_pga THEN max_vert.max_pga / max_hori.max_pga ELSE max_hori.max_pga / max_vert.max_pga END ratio,
        max_vert.max_pga AS max_vertical,
        max_hori.max_pga AS max_horizontal
FROM
(
        SELECT
		sourcepk,
    		ROUND(MAX(pga), 8) AS max_pga
    	FROM
		impact.pga
       	WHERE
        	vertical = true
       	GROUP BY
        	sourcepk
) max_vert INNER JOIN
(
        SELECT
		sourcepk,
    		ROUND(MAX(pga), 8) AS max_pga
    	FROM
		impact.pga
       	WHERE
        	vertical = false
       	GROUP BY
        	sourcepk
) max_hori ON max_vert.sourcepk = max_hori.sourcepk
RIGHT OUTER JOIN impact.source loc ON loc.sourcepk = max_hori.sourcepk
ORDER BY
    	ratio DESC NULLS LAST
LIMIT 10`
)

var (
    trace *log.Logger
    db *sql.DB
    dir string
)

func init() {

        file, err := os.OpenFile("/tmp/strong_motion_noise_check.log", os.O_RDWR|os.O_CREATE, 0666)
        if err != nil {
                fmt.Println("Failed initializing logfile:", err)
                os.Exit(1)
        }

        trace = log.New(file, "", log.LstdFlags|log.Lshortfile)
        dir = "/tmp"
}

func main() {
        // Could set all of these to be environment variables
        passwd, ok := os.LookupEnv("HAZARD_PASSWD")
        if !ok {
                trace.Fatalln("HAZARD_PASSWD not set for environment.")
        }
        db, err := sql.Open("postgres",
                "postgres://hazard_r:" + passwd + "@geonet-api-ng-read.ccuclj9uvil4.ap-southeast-2.rds.amazonaws.com/hazard?sslmode=disable")

        if err != nil {
                trace.Fatalf("ERROR: problem with DB config: %s", err)
        }
        defer db.Close() // Pretty cool

        err = db.Ping()
	if err != nil {
                log.Fatalf("ERROR: Can't contact DB: %s", err)
        }

        trace.Println("Getting top noise counts for Strong Motion")
        noiseCount(db)

        trace.Println("Getting PGV ratio difference for Strong Motion")
        ratioDiff(db)
}

/* https://wiki.geonet.org.nz/display/dmcops/Strong+Motion+Noise+checks#StrongMotionNoisechecks-ConstantReportingCountNoise */
func noiseCount(db *sql.DB) {
        rows, err := db.Query(noiseCountSQL)

        if err != nil {
                trace.Fatalf("Error: %s", err)
        }

        var (
                timestamp string
                station string
                blacklist string
                component string
                count int
        )

        file, err := os.OpenFile(filepath.Join(dir,"noiseCount.csv"), os.O_RDWR|os.O_APPEND|os.O_CREATE, 0666)
        if err != nil {
                trace.Fatalf("Failed opening file: %s", err)
        }
        defer file.Close()

        for rows.Next() {
                err := rows.Scan(&timestamp, &station, &blacklist, &component, &count)
                if err != nil {
                        trace.Fatalf("Error Scanning rows: %s", err)
                }

                file.WriteString(fmt.Sprintf("%s,%s,%s,%s,%d\n", timestamp, station, blacklist, component, count))
        }
}

/* https://wiki.geonet.org.nz/display/dmcops/Strong+Motion+Noise+checks#StrongMotionNoisechecks-PGAVerticalversusPGAHorizontalRatioNoise */
func ratioDiff(db *sql.DB) {

        rows, err := db.Query(ratioDiffSQL)
        if err != nil {
                trace.Fatalf("Error: %s", err)
        }

        var (
                timestamp string
                station string
                blacklist string
                ratio float64
                maxVertical float64
                maxHorizontal float64
        )

        file, err := os.OpenFile(filepath.Join(dir,"ratioDiff.csv"), os.O_RDWR|os.O_APPEND|os.O_CREATE, 0666)
        if err != nil {
                trace.Fatalf("Failed opening file: %s", err)
        }
        defer file.Close()

        for rows.Next() {
                err := rows.Scan(&timestamp, &station, &blacklist, &ratio, &maxVertical, &maxHorizontal)
                if err != nil {
                   trace.Fatalf("Error Scanning rows: %s", err)
                }
                file.WriteString(fmt.Sprintf("%s,%s,%s,%f,%f,%f\n", timestamp, station, blacklist, ratio, maxVertical, maxHorizontal))
        }
}
