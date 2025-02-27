package org.apache.atlas.auth.client.keycloak;

import org.apache.atlas.auth.client.config.AuthConfig;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.commons.collections.CollectionUtils;
import org.keycloak.representations.idm.*;
import org.keycloak.representations.oidc.TokenMetadataRepresentation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Keycloak client, deals with token creation refresh.
 */
public final class AtlasKeycloakClient {

    public final static Logger LOG = LoggerFactory.getLogger(AtlasKeycloakClient.class);

    private static KeycloakRestClient KEYCLOAK;
    private static AtlasKeycloakClient KEYCLOAK_CLIENT;

    private AtlasKeycloakClient() {
    }

    public List<UserRepresentation> searchUserByUserName(String username) throws AtlasBaseException {
        return KEYCLOAK.searchUserByUserName(username).body();
    }

    public Set<UserRepresentation> getRoleUserMembers(String roleName, int start, int size) throws AtlasBaseException {
        return KEYCLOAK.getRoleUserMembers(roleName, start, size).body();
    }

    public List<UserRepresentation> getRoleUserMembers(String roleName) throws AtlasBaseException {
        return KEYCLOAK.getRoleUserMembers(roleName).body();
    }

    public List<UserRepresentation> getAllUsers() throws AtlasBaseException {
        int start = 0;
        int size = 500;
        boolean found = true;

        List<UserRepresentation> ret = new ArrayList<>(0);

        do {
            List<UserRepresentation> userRepresentations = KEYCLOAK.getAllUsers(start, size).body();
            if (CollectionUtils.isNotEmpty(userRepresentations)) {
                ret.addAll(userRepresentations);
                start += size;
            } else {
                found = false;
            }

        } while (found && ret.size() % size == 0);

        return ret;
    }

    public List<GroupRepresentation> searchGroupByName(String groupName, Integer start, Integer size) throws AtlasBaseException {
        return KEYCLOAK.searchGroupByName(groupName, start, size).body();
    }

    public List<GroupRepresentation> getRoleGroupMembers(String roleName) throws AtlasBaseException {
        return KEYCLOAK.getRoleGroupMembers(roleName).body();
    }

    public Set<GroupRepresentation> getRoleGroupMembers(String roleName, Integer start, Integer size) throws AtlasBaseException {
        return KEYCLOAK.getRoleGroupMembers(roleName, start, size).body();
    }

    public List<GroupRepresentation> getGroupsForUserById(String userId) throws AtlasBaseException {
        return KEYCLOAK.getGroupsForUserById(userId).body();
    }

    public void addRealmLevelRoleMappingsForGroup(String groupId, List<RoleRepresentation> roles) throws AtlasBaseException {
        KEYCLOAK.addRealmLevelRoleMappingsForGroup(groupId, roles);
    }

    public void deleteRealmLevelRoleMappingsForGroup(String groupId, List<RoleRepresentation> roles) throws AtlasBaseException {
        KEYCLOAK.deleteRealmLevelRoleMappingsForGroup(groupId, roles);
    }

    public List<RoleRepresentation> getAllRoles() throws AtlasBaseException {
        int start = 0;
        int size = 500;
        boolean found = true;

        List<RoleRepresentation> ret = new ArrayList<>(0);
        do {
            List<RoleRepresentation> roleRepresentations = KEYCLOAK.getAllRoles(start, size).body();
            if (CollectionUtils.isNotEmpty(roleRepresentations)) {
                ret.addAll(roleRepresentations);
                start += size;
            } else {
                found = false;
            }
        } while (found && ret.size() % size == 0);

        return ret;
    }

    public void deleteRoleById(String roleId) throws AtlasBaseException {
        KEYCLOAK.deleteRoleById(roleId);
    }

    public void deleteRoleByName(String roleName) throws AtlasBaseException {
        KEYCLOAK.deleteRoleByName(roleName);
    }


    public List<RoleRepresentation> addRealmLevelRoleMappingsForUser(String userId, List<RoleRepresentation> roles) throws AtlasBaseException {
        return KEYCLOAK.addRealmLevelRoleMappingsForUser(userId, roles).body();
    }

    public void deleteRealmLevelRoleMappingsForUser(String userId, List<RoleRepresentation> roles) throws AtlasBaseException {
        KEYCLOAK.deleteRealmLevelRoleMappingsForUser(userId, roles);
    }

    public void createRole(RoleRepresentation roleRepresentation) throws AtlasBaseException {
        KEYCLOAK.createRole(roleRepresentation);
    }

    public void updateRole(String roleId, RoleRepresentation roleRepresentation) throws AtlasBaseException {
        KEYCLOAK.updateRole(roleId, roleRepresentation);
    }


    public RoleRepresentation getRoleById(String roleId) throws AtlasBaseException {
        return KEYCLOAK.getRoleById(roleId).body();
    }

    public RoleRepresentation getRoleByName(String roleName) throws AtlasBaseException {
        return KEYCLOAK.getRoleByName(roleName).body();
    }

    public Set<RoleRepresentation> getRoleComposites(String roleName) throws AtlasBaseException {
        return KEYCLOAK.getRoleComposites(roleName).body();
    }

    public void addComposites(String roleName, List<RoleRepresentation> roles) throws AtlasBaseException {
        KEYCLOAK.addComposites(roleName, roles);
    }

    public void deleteComposites(String roleName, List<RoleRepresentation> roles) throws AtlasBaseException {
        KEYCLOAK.deleteComposites(roleName, roles);
    }

    public List<AdminEventRepresentation> getAdminEvents(List<String> operationTypes, String authRealm, String authClient, String authUser, String authIpAddress, String resourcePath, String dateFrom, String dateTo, Integer first, Integer max) throws AtlasBaseException {
        return KEYCLOAK.getAdminEvents(operationTypes, authRealm, authClient, authUser, authIpAddress, resourcePath, dateFrom, dateTo, first, max).body();
    }

    public List<EventRepresentation> getEvents(List<String> type, String client, String user, String dateFrom, String dateTo, String ipAddress, Integer first, Integer max) throws AtlasBaseException {
        return KEYCLOAK.getEvents(type, client, user, dateFrom, dateTo, ipAddress, first, max).body();
    }

    public TokenMetadataRepresentation introspectToken(String token) throws AtlasBaseException {
        return KEYCLOAK.introspectToken(token).body();
    }

    public static AtlasKeycloakClient getKeycloakClient() throws AtlasBaseException {
        if (Objects.isNull(KEYCLOAK_CLIENT)) {
            LOG.info("Initializing Keycloak client..");
            try {
                init(AuthConfig.getConfig());
            } catch (Exception e) {
                LOG.error("Failed to connect to Keycloak {}", e.getMessage());
                throw new AtlasBaseException(AtlasErrorCode.KEYCLOAK_INIT_FAILED, e.getMessage());
            }

            LOG.info("Initialized Keycloak client..");
        }

        return KEYCLOAK_CLIENT;
    }

    private static void init(AuthConfig config) {
        synchronized (AtlasKeycloakClient.class) {
            if (KEYCLOAK_CLIENT == null) {
                KEYCLOAK = new KeycloakRestClient(config);
                KEYCLOAK_CLIENT = new AtlasKeycloakClient();
            }
        }
    }
}
