package googlestorage

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/betas-in/logger"
	"github.com/betas-in/utils"
)

func TestUploader(t *testing.T) {
	log := logger.NewLogger(3, true)
	bucket := "networth.leftshift.io"
	timeout, _ := time.ParseDuration("10m")

	fileName := "amfi-04-Jan-2022.txt"

	u, err := NewGCStorage(bucket, timeout, log)
	utils.Test().Nil(t, err)

	exists, err := u.Exists(fileName)
	utils.Test().Nil(t, err)
	utils.Test().Equals(t, true, exists)

	err = u.Upload(
		fmt.Sprintf("./testdata/%s", fileName),
		fileName,
	)
	utils.Test().Nil(t, err)

	path, err := u.Download(
		fileName,
		fileName,
	)
	utils.Test().Nil(t, err)
	utils.Test().Contains(t, path, fileName)

	path, err = u.Download(
		fmt.Sprintf("%s2", fileName),
		fmt.Sprintf("%s2", fileName),
	)
	utils.Test().Nil(t, err)
	utils.Test().Equals(t, "", path)

	_ = os.Remove(path)
}
