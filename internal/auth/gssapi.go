package auth

import (
	"context"
	"fmt"
)

// ErrGSSAPINotSupported indicates Kerberos/GSSAPI is not yet implemented in pure stdlib builds.
var ErrGSSAPINotSupported = fmt.Errorf("auth: GSSAPI/Kerberos SASL is not yet supported (see docs/CAPABILITIES.md)")

func gssapi(ctx context.Context, conn requester, sec Config) error {
	_ = ctx
	_ = conn
	if sec.SASL.Kerberos.Principal == "" {
		return fmt.Errorf("auth: GSSAPI requires KerberosConfig.Principal")
	}
	return ErrGSSAPINotSupported
}
