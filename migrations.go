package main

import (
	"errors"
	"flag"
	"fmt"
	"github.com/go-pg/pg/v9"
	"github.com/go-pg/pg/v9/orm"
	"golang.org/x/crypto/bcrypt"
	"golang.org/x/net/context"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type config struct {
	PostgresAddress        string
	PostgresDatabase       string
	PostgresUsername       string
	PostgresPassword       string
	MigrationDirectoryPath string
	Command                string
}

/**
File Name Convention for upgrading files: V1__initial_step_up.up.sql
File Name Convention for downgrading files: V1__initial_step_up.down.sql
Version - The migration version (numerical) of the migration file. (1 for the above example)
Processed - flag to mark a migration as processed. Processed here means attempted to migrate.
Success - flag to mark a migration as successfully applied.
*/
type SchemaMigration struct {
	ID            int
	Version       int    `pg:",notnull,unique:idx_unique_schema_version_migration_type"`
	Description   string `pg:",notnull"`
	MigrationType string `pg:",notnull,unique:idx_unique_schema_version_migration_type"`
	FilePath      string `pg:",notnull"`
	ExecutionTime int    `pg:",notnull,use_zero"`
	Processed     bool   `pg:",notnull,use_zero"`
	FileContent   string `pg:",notnull"`
	CheckSum      string `pg:",notnull"`
	Success       bool   `pg:",notnull,use_zero"`
	Error         string
	CreatedAt     time.Time `pg:"default:now(),notnull"`
	UpdatedAt     time.Time `pg:",notnull"`
}

type SchemaMigrationDao struct {
	db *pg.DB
}

func NewSchemaMigrationDao(db *pg.DB) *SchemaMigrationDao {
	return &SchemaMigrationDao{db: db}
}

type FileDetails struct {
	Version       int
	Description   string
	MigrationType string
	FilePath      string
	FileContent   string
	CheckSum      string
}

var (
	schemaMigrationDao *SchemaMigrationDao
	conf               config
)

func init() {
	flag.StringVar(&conf.PostgresAddress, "postgres_address", "localhost:5432", "Postgres connection format {string IP:port} or {URL:post}")
	flag.StringVar(&conf.PostgresDatabase, "postgres_database", "saas", "Postgres database name")
	flag.StringVar(&conf.PostgresUsername, "postgres_username", "saas", "Postgres database username")
	flag.StringVar(&conf.PostgresPassword, "postgres_password", "saas", "Postgres database password")
	flag.StringVar(&conf.MigrationDirectoryPath, "migration_directory_path", "/Users/akcps/go/src/pg-migrations/sql", "Migration directory path")
	flag.StringVar(&conf.Command, "command", "version", "up: runs all available migrations \ndown: reverts last migration \nreset:reverts all migrations \nversion:prints current db version\n")
}
func main() {
	var err error
	flag.Parse()

	fmt.Println("postgres_address:", conf.PostgresAddress)
	fmt.Println("postgres_database:", conf.PostgresDatabase)
	fmt.Println("postgres_username:", conf.PostgresUsername)
	fmt.Println("postgres_password", conf.PostgresPassword)
	fmt.Println("migration_directory_path:", conf.MigrationDirectoryPath)
	fmt.Println("command", conf.Command)
	db, err := connectToDB(conf.PostgresAddress, conf.PostgresDatabase, conf.PostgresUsername, conf.PostgresPassword)
	db.AddQueryHook(dbLogger{})
	if err != nil {
		log.Panic("Unable to connect to postgres.")
	}
	if err := checkDBHealth(db); err != nil {
		log.Panic("Unable to connect to postgres.")
	}
	createSchemaForMigration(db)
	schemaMigrationDao = NewSchemaMigrationDao(db)

	switch conf.Command {
	case "version":
		getCurrentVersion()
		break
	case "up":
		upgradeMigration()
		break
	case "down":
		downgradeMigration()
		break
	case "reset":
		revertAll()
		break
	default:
		fmt.Println("Unrecognized command. Accepted commands - up/down/version/reset.")
	}

}

func getCurrentVersion() {
	latestMigration, err := schemaMigrationDao.GetCurrentVersion()
	if err != nil {
		log.Printf("Unable to fetch Current Version.Error: %v", err.Error())
	} else {
		log.Printf("Current Version %v", latestMigration.Version)
	}
}

func upgradeMigration() {
	if err := syncLatestMigrations(conf.MigrationDirectoryPath); err != nil {
		log.Printf("Unable to sync the migration directory %v with the database. Error %v",
			conf.MigrationDirectoryPath, err.Error())
	}
}

func downgradeMigration() {
	latestMigration, err := schemaMigrationDao.GetCurrentVersion()
	if err != nil {
		log.Printf("Unable to fetch Current Version.Error: %v", err.Error())
		os.Exit(1)
	}
	if err := revertMigration(latestMigration.Version); err != nil {
		log.Printf("Unable to revert migration. Error %v", err.Error())
	}
}

func revertAll() {
	unProcessedMigrations, err := schemaMigrationDao.FetchUnProcessedSchemaMigrations("DOWNGRADE", "id DESC")
	if err != nil {
		log.Printf("Unable to reset. Error %v", err.Error())
	}
	for _, migrationSchema := range unProcessedMigrations {
		if err = ApplyMigration(&migrationSchema); err != nil {
			log.Printf("Unable to rever %#v. Error %v", migrationSchema, err.Error())
		}
	}
}

func revertMigration(version int) error {
	sm, err := schemaMigrationDao.FetchUnProcessedSchemaMigrationBasedOnVersionAndType(version, "DOWNGRADE")
	if err != nil {
		return err
	}
	err = ApplyMigration(sm)
	if err != nil {
		return err
	}
	return nil
}

func syncLatestMigrations(migrationRootDirectory string) error {
	var err error
	err = AddNewSchemaMigrations(migrationRootDirectory, schemaMigrationDao)
	if err != nil {
		return err
	}
	unProcessedMigrations, err := schemaMigrationDao.FetchUnProcessedSchemaMigrations("UPGRADE", "id ASC")
	if err != nil {
		return err
	}
	for _, migrationSchema := range unProcessedMigrations {
		if err = ApplyMigration(&migrationSchema); err != nil {
			return err
		}
	}
	return nil
}

func ApplyMigration(migrationSchema *SchemaMigration) error {
	start := time.Now()
	log.Printf("Applyling schema %#v ", migrationSchema.FilePath)
	_, err := schemaMigrationDao.ApplySchemaMigration(migrationSchema)
	end := time.Now()
	executionTime := int(end.Sub(start).Seconds())
	if err != nil {
		_, _ = schemaMigrationDao.UpdateSchemaMigration(migrationSchema.ID, executionTime, true, false, err.Error())
		log.Printf("Unable to apply schemas %#v. Error %v", migrationSchema.FilePath, err.Error())
		return err
	}
	_, err = schemaMigrationDao.UpdateSchemaMigration(migrationSchema.ID, executionTime, true, true, "")
	if err != nil {
		log.Printf("Unable to update schemas %#v. Error %v", migrationSchema.FilePath, err.Error())
		return err
	}
	log.Printf("Schema %#v  applied successfully.", migrationSchema.FilePath)
	return nil
}

// This function scans the migrations files directory and creates entry into the migrations table.
func AddNewSchemaMigrations(migrationRootDirectory string, schemaMigrationDao *SchemaMigrationDao) error {
	var files []string
	var err error
	err = filepath.Walk(migrationRootDirectory, func(path string, info os.FileInfo, err error) error {
		files = append(files, path)
		return err
	})
	if err != nil {
		log.Println(err)
		return err
	}
	for _, filePath := range files {
		if filePath == migrationRootDirectory {
			// skip the directory
			continue
		}
		log.Printf("Processing ........... %v", filePath)
		count, err := schemaMigrationDao.CountSchemaMigrationForFilePath(filePath)
		if err != nil {
			log.Printf("Error will processing %v", filePath)
			return err
		}
		if count == 1 {
			// file is already present, no-op
			log.Printf("Skipping... Entry already present for %v", filePath)
			continue
		}
		sm, err := AddEntryToDB(filePath)
		if err != nil {
			log.Printf("Error will processing %v", filePath)
			return err
		}
		log.Printf("Added SchemaMigration %#v for filepath %#v", sm, filePath)
	}
	return nil
}

func AddEntryToDB(filePath string) (*SchemaMigration, error) {
	fileDetails, err := GetFileDetails(filePath)
	if err != nil {
		return nil, err
	}
	now := time.Now()
	sm := &SchemaMigration{
		Version:       fileDetails.Version,
		Description:   fileDetails.Description,
		MigrationType: fileDetails.MigrationType,
		FilePath:      fileDetails.FilePath,
		ExecutionTime: 0,
		Processed:     false,
		FileContent:   fileDetails.FileContent,
		CheckSum:      fileDetails.CheckSum,
		Success:       false,
		CreatedAt:     now,
		UpdatedAt:     now,
	}
	sm, err = schemaMigrationDao.AddSchemaMigration(sm)
	if err != nil {
		return nil, err
	}
	return sm, nil
}

func connectToDB(url, databaseName, username, password string) (*pg.DB, error) {
	db := pg.Connect(&pg.Options{
		Database: databaseName,
		User:     username,
		Password: password,
		Addr:     url,
	})
	return db, nil
}

func createSchemaForMigration(db *pg.DB) {
	for _, model := range []interface{}{&SchemaMigration{}} {
		if err := db.CreateTable(model, &orm.CreateTableOptions{
			IfNotExists:   true,
			FKConstraints: true,
		}); err != nil {
			log.Fatalf("Unable to create tables %v", err.Error())
		}
	}
}

func checkDBHealth(db *pg.DB) error {
	_, err := db.Exec("SELECT 1")
	return err
}

func (d *SchemaMigrationDao) CountSchemaMigrationForFilePath(filePath string) (int, error) {
	log.Printf("CountSchemaMigrationForFilePath request with: %#v", filePath)
	schemaMigration := new(SchemaMigration)
	count, err := d.db.Model(schemaMigration).Where("file_path = ?", filePath).Count()
	if err != nil {
		return count, err
	}
	log.Printf("CountSchemaMigrationForFilePath response with count %v", count)
	return count, nil
}

func (d *SchemaMigrationDao) AddSchemaMigration(sm *SchemaMigration) (*SchemaMigration, error) {
	log.Printf("AddSchemaMigration request with: %#v", sm)

	err := d.db.Insert(sm)
	if err != nil {
		return nil, err
	}
	log.Printf("AddSchemaMigration response with: %#v", sm)
	return sm, nil
}

func (d *SchemaMigrationDao) FetchUnProcessedSchemaMigrations(migrationType, order string) ([]SchemaMigration, error) {
	log.Printf("FetchUnProcessedSchemaMigrations request with migrationType %#v, order %#v", migrationType, order)
	var schemaMigrations []SchemaMigration
	err := d.db.Model(&schemaMigrations).Where("processed = ? and migration_type = ?", false, migrationType).Order(order).Select()
	if err != nil {
		return nil, err
	}
	log.Printf("FetchUnProcessedSchemaMigrations response for migrationType %v  %#v", migrationType, schemaMigrations)
	return schemaMigrations, nil
}

func (d *SchemaMigrationDao) UpdateSchemaMigration(ID, executionTime int, processed, success bool, e string) (*SchemaMigration, error) {
	log.Printf("UpdateSchemaMigration request with ID %v executionTime %v processed %v, success %v , error %v", ID, executionTime, processed, success, e)
	var columns []string
	columns = append(columns, "execution_time")
	columns = append(columns, "processed")
	columns = append(columns, "success")
	columns = append(columns, "error")
	schemaMigration := &SchemaMigration{
		ID:            ID,
		ExecutionTime: executionTime,
		Processed:     processed,
		Success:       success,
		UpdatedAt:     time.Now(),
		Error:         e,
	}
	_, err := d.db.Model(schemaMigration).Column(columns...).Where("id = ?", ID).Update()
	if err != nil {
		log.Printf("Unable to update schema migration. Error: %#v", err.Error())
		return nil, err
	}
	log.Printf("UpdateSchemaMigration response with ID %v   %#v", ID, schemaMigration)
	return schemaMigration, nil
}

func (d *SchemaMigrationDao) ApplySchemaMigration(sm *SchemaMigration) (*SchemaMigration, error) {
	log.Printf("ApplySchemaMigration request with: %#v", sm)
	tx, _ := d.db.Begin()
	res, err := tx.Exec(sm.FileContent)
	if err != nil {
		_ = tx.Rollback()
		return nil, err
	}
	_ = tx.Commit()
	log.Printf("ApplySchemaMigration response with: %#v result: %#v", sm, res)
	return sm, nil
}

func GetFileDetails(filePath string) (*FileDetails, error) {
	var err error
	var invalidFileNameErr error = errors.New(fmt.Sprintf("File Name structure is invalid  %v", filePath))
	fileContent, err := ioutil.ReadFile(filePath)
	if err != nil {
		return nil, err
	}
	fileStrs := strings.Split(filePath, "/")
	fileStrs = strings.Split(fileStrs[len(fileStrs)-1], ".")
	if len(fileStrs) != 3 {
		return nil, invalidFileNameErr
	}
	var migrationType string
	switch fileStrs[1] {
	case "up":
		{
			migrationType = "UPGRADE"
			break
		}
	case "down":
		{
			migrationType = "DOWNGRADE"
			break
		}
	default:
		return nil, invalidFileNameErr
	}

	fileStrs = strings.Split(fileStrs[0], "__")
	if len(fileStrs) != 2 {
		return nil, invalidFileNameErr
	}
	description := fileStrs[1]
	versionStr := fileStrs[0][1:]
	version, err := strconv.Atoi(versionStr)
	if err != nil {
		return nil, invalidFileNameErr
	}
	hash, err := bcrypt.GenerateFromPassword(fileContent, bcrypt.DefaultCost)
	if err != nil {
		return nil, invalidFileNameErr
	}
	return &FileDetails{
		Version:       version,
		Description:   description,
		MigrationType: migrationType,
		FilePath:      filePath,
		FileContent:   string(fileContent),
		CheckSum:      string(hash),
	}, nil
}

func (d *SchemaMigrationDao) FetchUnProcessedSchemaMigrationBasedOnVersionAndType(ID int, migrationType string) (*SchemaMigration, error) {
	log.Printf("FetchUnProcessedSchemaMigrationBasedOnVersionAndType request with ID %v migrationType %#v", ID, migrationType)
	schemaMigration := new(SchemaMigration)
	err := d.db.Model(schemaMigration).Where("id = ? and processed = ? and migration_type = ?", ID, false, migrationType).Select()
	if err != nil {
		return nil, err
	}
	log.Printf("FetchUnProcessedSchemaMigrations response for ID %v, migrationType %v  %#v", ID, migrationType, schemaMigration)
	return schemaMigration, nil
}

func (d *SchemaMigrationDao) GetCurrentVersion() (*SchemaMigration, error) {
	log.Println("GetCurrentVersion request")
	schemaMigration := new(SchemaMigration)
	err := d.db.Model(schemaMigration).Where("processed = ? and migration_type = ?", true, "UPGRADE").Last()
	if err != nil {
		return nil, err
	}
	log.Printf("GetCurrentVersion response with  %#v", schemaMigration)
	return schemaMigration, nil
}

type dbLogger struct{}

func (d dbLogger) BeforeQuery(c context.Context, q *pg.QueryEvent) (context.Context, error) {
	return c, nil
}

func (d dbLogger) AfterQuery(c context.Context, q *pg.QueryEvent) error {
	log.Println(q.FormattedQuery())
	return nil
}
