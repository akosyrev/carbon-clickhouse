package writer

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"go.uber.org/zap"
)

func HardLink(filename string, hardlink_dir string, hardlink_suffix string) error {
	d, fn := filepath.Split(filename)

	var err error

	if _, err = os.Stat(filepath.Join(d, hardlink_dir)); os.IsNotExist(err) {
		//fmt.Println("HARDLINK: dir does not exist ")
		_ = true // Needs more handling?
		err = os.Mkdir(filepath.Join(d, hardlink_dir), 0755)
		if err != nil {
			//fmt.Printf("HARDLINK: %s\n", err)
			_ = true // Needs more handling?
			return err
		}
	}

	if _, err := os.Stat(filepath.Join(d, hardlink_dir, fn)); !os.IsNotExist(err) {
		_ = true // Needs more handling?

	}

	if _, err := os.Stat(filepath.Join(d, hardlink_dir, "_"+fn)); !os.IsNotExist(err) {
		_ = true // Needs more handling?
	}

	hlFrom := filename
	ext := filepath.Ext(filename)
	fname := strings.TrimSuffix(filepath.Base(filename), ext)
	hlTo := filepath.Join(d, hardlink_dir, fname+hardlink_suffix+ext)
	//fmt.Println(hlFrom, hlTo)
	err = os.Link(hlFrom, hlTo)
	if err != nil {
		//fmt.Printf("HARDLINK: error creating, %s\n", err)
		return err
	}
	//fmt.Println("HARDLINK: link created")

	return nil
}

func (w *Writer) HardLinkAll() error {
	flist, err := ioutil.ReadDir(w.path)
	if err != nil {
		w.logger.Error("ReadDir failed", zap.Error(err))
		return err
	}

	for _, f := range flist {
		if f.IsDir() {
			continue
		}
		if !strings.HasPrefix(f.Name(), "default.") {
			continue
		}

		if err := HardLink(filepath.Join(w.path, f.Name()), w.hardlinksPath, w.hardlinksSuffix); err != nil {
			return err
		}
	}

	return nil
}
