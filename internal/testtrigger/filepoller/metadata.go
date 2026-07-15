package filepoller

import (
	"github.com/project-flogo/core/data/coerce"
)

// HandlerSettings mirror the TIBCO File Poller trigger handler settings.
type HandlerSettings struct {
	PollingDirectory string `md:"pollingDir,required"`
	Recursive        bool   `md:"recursive,required"`
	FileFilter       string `md:"fileFilter"`
	PollingInterval  int    `md:"pollingInterval,required"`
	Mode             string `md:"mode,required"`
	FileEvents       string `md:"fileEvents"`
}

// Output corresponds to the trigger output object.
type Output struct {
	Output map[string]interface{} `md:"output"`
}

// ToMap converts Output struct to map.
func (o *Output) ToMap() map[string]interface{} {
	return map[string]interface{}{
		"output": o.Output,
	}
}

// FromMap converts a map to Output struct.
func (o *Output) FromMap(values map[string]interface{}) error {
	var err error
	o.Output, err = coerce.ToObject(values["output"])
	if err != nil {
		return err
	}
	return nil
}
