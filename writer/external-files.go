package writer

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
)

func getExternalFiles(dir string, mask string) ([]string, error) {
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	res := []string{}
	for _, f := range files {
		if !f.IsDir() && strings.Contains(f.Name(), mask) {
			res = append(res, f.Name())
		}
	}
	return res, nil
}

func LinkExternal(fn string, tables []string, mask string) error {
	var err error
	var extFiles []string

	d := filepath.Dir(fn)

	if extFiles, err = getExternalFiles(d, mask); err != nil {
		return err
	}
	for _, extf := range extFiles {
		for _, t := range tables {

			if _, err = os.Stat(filepath.Join(d, t)); os.IsNotExist(err) {
				err = os.Mkdir(filepath.Join(d, t), 0755)
				if err != nil {
					return err
				}
			}

			if _, err := os.Stat(filepath.Join(d, t, extf)); !os.IsNotExist(err) {
				// symlink or file already exists
				continue
			}

			if _, err := os.Stat(filepath.Join(d, t, "_"+extf)); !os.IsNotExist(err) {
				// finished symlink or file already exists
				continue
			}

			err = os.Symlink(filepath.Join(d, extf), filepath.Join(d, t, extf))
			if err != nil {
				return err
			}
		}
	}
	return nil
}
