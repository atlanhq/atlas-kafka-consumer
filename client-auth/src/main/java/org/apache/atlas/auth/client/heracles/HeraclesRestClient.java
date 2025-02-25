package org.apache.atlas.auth.client.heracles;

import org.apache.atlas.auth.client.config.AuthConfig;
import org.apache.atlas.auth.client.auth.AbstractAuthClient;
import org.apache.atlas.auth.client.heracles.models.HeraclesRoleViewRepresentation;
import org.apache.atlas.auth.client.heracles.models.HeraclesUserViewRepresentation;
import org.apache.atlas.exception.AtlasBaseException;
import retrofit2.Response;

import java.util.List;

public class HeraclesRestClient extends AbstractAuthClient {

    public HeraclesRestClient(final AuthConfig authConfig) {
        super(authConfig);
    }
    public Response<List<HeraclesUserViewRepresentation>> getUsersMappings(int offset, int limit, String sort, String[] columns) throws AtlasBaseException {
        return processResponse(this.retrofitHeraclesClient.getUsersMapping(offset, limit,sort, columns));
    }

    public Response<List<HeraclesRoleViewRepresentation>> getRolesMappings(int offset, int limit, String sort, String[] columns) throws AtlasBaseException {
        return processResponse(this.retrofitHeraclesClient.getRolesMapping(offset, limit, sort, columns));
    }

}