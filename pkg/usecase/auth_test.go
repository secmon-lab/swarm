package usecase_test

/*
func TestAuthorize(t *testing.T) {
	idToken, ok := os.LookupEnv("TEST_GOOGLE_ID_TOKEN")
	if !ok {
		t.Skip("TEST_GOOGLE_ID_TOKEN is not set")
	}
	email, ok := os.LookupEnv("TEST_GOOGLE_CLAIM_EMAIL")
	if !ok {
		t.Skip("TEST_GOOGLE_CLAIM_EMAIL is not set")
	}

	testCases := map[string]struct {
		idToken string
		auth    *config.Auth
		ok      bool
	}{
		"AllowAll": {
			idToken: idToken,
			auth: &config.Auth{
				AllowAll: true,
			},
			ok: true,
		},
		"Allowed": {
			idToken: idToken,
			auth: &config.Auth{
				AllowAll: false,
				Allowed:  &[]string{email},
			},
			ok: true,
		},
		"NotAllowed": {
			idToken: idToken,
			auth: &config.Auth{
				AllowAll: false,
				Allowed:  &[]string{"xxx@example.com"},
			},
			ok: false,
		},
		"EmptyToken": {
			idToken: "",
			auth: &config.Auth{
				AllowAll: false,
				Allowed:  &[]string{email},
			},
			ok: false,
		},
		"EmptyAuth": {
			idToken: "",
			auth:    nil,
			ok:      true,
		},
		"EmptyAuthWithToken": {
			idToken: idToken,
			auth:    nil,
			ok:      true,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			cfg := &config.Config{
				Auth: tc.auth,
			}
			uc := usecase.New(cfg, infra.New())
			authCtx, err := uc.Authorize(ctx, []byte(tc.idToken))

			if tc.ok {
				gt.V(t, authCtx).NotNil()
				gt.NoError(t, err)
			} else {
				gt.V(t, authCtx).Nil()
				gt.Error(t, err)
			}
		})
	}

}
*/
