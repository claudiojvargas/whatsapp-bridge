package contacts

import (
	"context"
	"fmt"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const (
	contentBinary  = "/system/bin/content"
	rawContactsURI = "content://com.android.contacts/raw_contacts"
	dataURI        = "content://com.android.contacts/data"

	nameMIME  = "vnd.android.cursor.item/name"
	phoneMIME = "vnd.android.cursor.item/phone_v2"

	phoneTypeMobile = 2
)

var rawContactIDPattern = regexp.MustCompile(`_id=(\d+)`)

type Writer struct{}

func New() *Writer {
	return &Writer{}
}

func (w *Writer) Create(
	ctx context.Context,
	name string,
	number string,
) (int64, error) {
	name = strings.TrimSpace(name)
	number = strings.TrimSpace(number)

	if name == "" {
		return 0, fmt.Errorf("contact name is required")
	}

	if number == "" {
		return 0, fmt.Errorf("contact number is required")
	}

	marker := fmt.Sprintf(
		"native-agent-%d",
		time.Now().UnixNano(),
	)

	_, err := runContent(
		ctx,
		"insert",
		"--uri",
		rawContactsURI,
		"--bind",
		"sourceid:s:"+marker,
	)

	if err != nil {
		return 0, fmt.Errorf(
			"create raw contact: %w",
			err,
		)
	}

	rawID, err := findRawContactID(
		ctx,
		marker,
	)

	if err != nil {
		return 0, err
	}

	if _, err := runContent(
		ctx,
		"insert",
		"--uri",
		dataURI,
		"--bind",
		fmt.Sprintf(
			"raw_contact_id:i:%d",
			rawID,
		),
		"--bind",
		"mimetype:s:"+nameMIME,
		"--bind",
		"data1:s:"+name,
	); err != nil {
		return 0, fmt.Errorf(
			"insert contact name: %w",
			err,
		)
	}

	if _, err := runContent(
		ctx,
		"insert",
		"--uri",
		dataURI,
		"--bind",
		fmt.Sprintf(
			"raw_contact_id:i:%d",
			rawID,
		),
		"--bind",
		"mimetype:s:"+phoneMIME,
		"--bind",
		"data1:s:"+number,
		"--bind",
		fmt.Sprintf(
			"data2:i:%d",
			phoneTypeMobile,
		),
	); err != nil {
		return 0, fmt.Errorf(
			"insert contact phone: %w",
			err,
		)
	}

	return rawID, nil
}

func findRawContactID(
	ctx context.Context,
	marker string,
) (int64, error) {
	output, err := runContent(
		ctx,
		"query",
		"--uri",
		rawContactsURI,
		"--projection",
		"_id:sourceid",
		"--where",
		"sourceid='"+marker+"'",
	)

	if err != nil {
		return 0, fmt.Errorf(
			"query raw contact: %w",
			err,
		)
	}

	match := rawContactIDPattern.FindStringSubmatch(
		output,
	)

	if len(match) != 2 {
		return 0, fmt.Errorf(
			"raw contact id not found: %s",
			output,
		)
	}

	id, err := strconv.ParseInt(
		match[1],
		10,
		64,
	)

	if err != nil {
		return 0, fmt.Errorf(
			"parse raw contact id: %w",
			err,
		)
	}

	return id, nil
}

func runContent(
	ctx context.Context,
	args ...string,
) (string, error) {
	command := exec.CommandContext(
		ctx,
		contentBinary,
		args...,
	)

	output, err := command.CombinedOutput()

	if err != nil {
		return "", fmt.Errorf(
			"content command failed: %w: %s",
			err,
			strings.TrimSpace(
				string(output),
			),
		)
	}

	return strings.TrimSpace(
		string(output),
	), nil
}
