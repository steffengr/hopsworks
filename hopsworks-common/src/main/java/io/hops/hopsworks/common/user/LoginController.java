package io.hops.hopsworks.common.user;

import io.hops.hopsworks.common.constants.auth.AuthenticationConstants;
import io.hops.hopsworks.common.constants.message.ResponseMessages;
import io.hops.hopsworks.common.dao.certificates.CertsFacade;
import io.hops.hopsworks.common.dao.certificates.ProjectGenericUserCerts;
import io.hops.hopsworks.common.dao.certificates.UserCerts;
import io.hops.hopsworks.common.dao.project.Project;
import io.hops.hopsworks.common.dao.project.ProjectFacade;
import io.hops.hopsworks.common.dao.user.BbcGroup;
import io.hops.hopsworks.common.dao.user.BbcGroupFacade;
import io.hops.hopsworks.common.dao.user.UserFacade;
import io.hops.hopsworks.common.dao.user.Users;
import io.hops.hopsworks.common.dao.user.security.audit.AccountAuditFacade;
import io.hops.hopsworks.common.dao.user.security.audit.AccountsAuditActions;
import io.hops.hopsworks.common.dao.user.security.audit.RolesAuditActions;
import io.hops.hopsworks.common.dao.user.security.audit.UserAuditActions;
import io.hops.hopsworks.common.dao.user.security.ua.PeopleAccountStatus;
import io.hops.hopsworks.common.dao.user.security.ua.PeopleAccountType;
import io.hops.hopsworks.common.dao.user.security.ua.SecurityQuestion;
import io.hops.hopsworks.common.dao.user.security.ua.SecurityUtils;
import io.hops.hopsworks.common.dao.user.security.ua.UserAccountsEmailMessages;
import io.hops.hopsworks.common.exception.AppException;
import io.hops.hopsworks.common.util.EmailBean;
import io.hops.hopsworks.common.util.HopsUtils;
import io.hops.hopsworks.common.util.Settings;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.ejb.EJB;
import javax.ejb.Stateless;
import javax.ejb.TransactionAttribute;
import javax.ejb.TransactionAttributeType;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Response;
import org.apache.commons.codec.digest.DigestUtils;

@Stateless
@TransactionAttribute(TransactionAttributeType.NEVER)
public class LoginController {

  private final static Logger LOGGER = Logger.getLogger(LoginController.class.getName());
  private final static int SALT_LENGTH = 64;
  private final static String DIGEST = "";
  private final static int RANDOM_PWD_LEN = 64;

  @EJB
  private UserFacade userFacade;
  @EJB
  private BbcGroupFacade bbcGroupFacade;
  @EJB
  private UserStatusValidator userStatusValidator;
  @EJB
  private Settings settings;
  @EJB
  private AccountAuditFacade accountAuditFacade;
  @EJB
  private EmailBean emailBean;
  @EJB
  private CertsFacade userCertsFacade;
  @EJB
  private ProjectFacade projectFacade;

  /**
   * Pre check for custom realm login.
   * @param email
   * @param password
   * @param otp
   * @param req
   * @return
   * @throws MessagingException
   * @throws AppException 
   */
  public String preCustomRealmLoginChecke(String email, String password, String otp, HttpServletRequest req) throws
      MessagingException, AppException {
    if (email == null || email.isEmpty() || password == null || password.isEmpty()) {
      throw new IllegalArgumentException("Missing argument.");
    }
    Users user = userFacade.findByEmail(email);
    if (user == null) {
      throw new IllegalArgumentException("User not found.");
    }
    if (isTwoFactorEnabled(user)) {
      if ((otp == null || otp.isEmpty()) && user.getMode().equals(PeopleAccountType.M_ACCOUNT_TYPE)) {
        if (checkPasswordAndStatus(user, password, req)) {
          throw new IllegalStateException("Second factor required.");
        }
      }
    }

    // Add padding if custom realm is disabled
    if (otp == null || otp.isEmpty() && user.getMode().equals(PeopleAccountType.M_ACCOUNT_TYPE)) {
      otp = AuthenticationConstants.MOBILE_OTP_PADDING;
    }
    String newPassword = getPasswordPlusSalt(password, user.getSalt());
    if (otp.length() == AuthenticationConstants.MOBILE_OTP_PADDING.length() && user.getMode().equals(
        PeopleAccountType.M_ACCOUNT_TYPE)) {
      newPassword = newPassword + otp;
    } else if (otp.length() == AuthenticationConstants.YUBIKEY_OTP_PADDING.length() && user.getMode().equals(
        PeopleAccountType.Y_ACCOUNT_TYPE)) {
      newPassword = newPassword + otp + AuthenticationConstants.YUBIKEY_USER_MARKER;
    } else {
      throw new IllegalArgumentException("Could not recognize the account type. Report a bug.");
    }
    return newPassword;
  }

  /**
   * Validates password and update account audit
   *
   * @param user
   * @param password
   * @param req
   * @return
   * @throws MessagingException
   */
  public boolean validatePassword(Users user, String password, HttpServletRequest req) throws MessagingException {
    if (user == null) {
      throw new IllegalArgumentException("User not set.");
    }
    String userPwdHash = getPasswordHash(user.getPassword(), user.getSalt());
    String pwdHash = getPasswordHash(password, user.getSalt());
    if (!userPwdHash.equals(pwdHash)) {
      registerFalseLogin(user, req);
      LOGGER.log(Level.WARNING, "False login attempt by user: {0}", user.getEmail());
      return false;
    }
    resetFalseLogin(user);
    return true;
  }

  /**
   * Validates password but will not update account audit
   *
   * @param user
   * @param password
   * @return
   * @throws MessagingException
   */
  public boolean validatePassword(Users user, String password) throws MessagingException {
    if (user == null) {
      throw new IllegalArgumentException("User not set.");
    }
    String userPwdHash = getPasswordHash(user.getPassword(), user.getSalt());
    String pwdHash = getPasswordHash(password, user.getSalt());
    if (!userPwdHash.equals(pwdHash)) {
      registerFalseLogin(user);
      LOGGER.log(Level.WARNING, "False login attempt by user: {0}", user.getEmail());
      return false;
    }
    resetFalseLogin(user);
    return true;
  }

  /**
   * Validate security question and update false login attempts   
   * @param user
   * @param securityQuestion
   * @param securityAnswer
   * @param req
   * @return
   * @throws AppException
   * @throws MessagingException 
   */
  public boolean validateSecurityQA(Users user, String securityQuestion, String securityAnswer, HttpServletRequest req)
      throws AppException, MessagingException {
    if (user == null) {
      throw new IllegalArgumentException("User not set.");
    }
    if (!user.getSecurityQuestion().getValue().equalsIgnoreCase(securityQuestion)
        || !user.getSecurityAnswer().equals(DigestUtils.sha256Hex(securityAnswer.toLowerCase()))) {
      registerFalseLogin(user, req);
      LOGGER.log(Level.WARNING, "False Security Question attempt by user: {0}", user.getEmail());
      throw new AppException(Response.Status.BAD_REQUEST.getStatusCode(), ResponseMessages.SEC_QA_INCORRECT);
    }
    return true;
  }

  /**
   * Checks password and user status. Also updates false login attempts
   * @param user
   * @param password
   * @param req
   * @return
   * @throws MessagingException
   * @throws AppException 
   */
  public boolean checkPasswordAndStatus(Users user, String password, HttpServletRequest req) throws MessagingException,
      AppException {
    if (user == null) {
      throw new IllegalArgumentException("User not set.");
    }
    if (!validatePassword(user, password, req)) {
      return false;
    }
    userStatusValidator.checkStatus(user.getStatus());
    return true;
  }

  /**
   * Validates email validation key. Also updates false key validation attempts.
   * @param key
   * @param req
   * @throws AppException 
   */
  public void validateKey(String key, HttpServletRequest req) throws AppException {
    if (key == null) {
      throw new AppException(Response.Status.BAD_REQUEST.getStatusCode(), "the validation key should not be null");
    }
    if (key.length() <= AuthenticationConstants.USERNAME_LENGTH) {
      throw new AppException(Response.Status.BAD_REQUEST.getStatusCode(), "The validation key is invalid");
    }
    String userName = key.substring(0, AuthenticationConstants.USERNAME_LENGTH);
    // get the 8 char username
    String secret = key.substring(AuthenticationConstants.USERNAME_LENGTH);
    Users user = userFacade.findByUsername(userName);
    if (user == null) {
      throw new AppException(Response.Status.BAD_REQUEST.getStatusCode(), "The user does not exist");
    }
    if (!secret.equals(user.getValidationKey())) {
      registerFalseKeyValidation(user, req);
      throw new AppException(Response.Status.BAD_REQUEST.getStatusCode(), "Wrong validation key");
    }

    if (!user.getStatus().equals(PeopleAccountStatus.NEW_MOBILE_ACCOUNT) && !user.getStatus().equals(
        PeopleAccountStatus.NEW_YUBIKEY_ACCOUNT)) {
      switch (user.getStatus()) {
        case VERIFIED_ACCOUNT:
          throw new AppException(Response.Status.BAD_REQUEST.getStatusCode(),
              "This user is already verified, but still need to be activated by the administrator");
        case ACTIVATED_ACCOUNT:
          throw new AppException(Response.Status.BAD_REQUEST.getStatusCode(), "This user is already verified");
        default:
          throw new AppException(Response.Status.BAD_REQUEST.getStatusCode(), "This user has been blocked");
      }
    }

    user.setStatus(PeopleAccountStatus.VERIFIED_ACCOUNT);
    userFacade.update(user);
    accountAuditFacade.registerRoleChange(user, PeopleAccountStatus.VERIFIED_ACCOUNT.name(), RolesAuditActions.SUCCESS.
        name(), "Account verification", user, req);
  }

  /**
   * Sends new activation key to the given user.
   * @param user
   * @param req
   * @throws MessagingException 
   */
  public void sendNewValidationKey(Users user, HttpServletRequest req) throws MessagingException {
    if (user == null) {
      throw new IllegalArgumentException("User not set.");
    }
    String activationKey = SecurityUtils.getRandomPassword(RANDOM_PWD_LEN);
    emailBean.sendEmail(user.getEmail(), Message.RecipientType.TO, UserAccountsEmailMessages.ACCOUNT_REQUEST_SUBJECT,
        UserAccountsEmailMessages.buildMobileRequestMessageRest(settings.getVerificationEndpoint(), user.getUsername()
            + activationKey));
    user.setValidationKey(activationKey);
    userFacade.update(user);
  }

  /**
   * Reset password with random string and send email to user with the new password.
   *
   * @param user
   * @param req
   * @throws AppException
   * @throws MessagingException
   * @throws Exception
   */
  public void resetPassword(Users user, HttpServletRequest req) throws AppException, MessagingException, Exception {
    if (user == null) {
      throw new IllegalArgumentException("User not set.");
    }
    if (userStatusValidator.isBlockedAccount(user)) {
      throw new IllegalStateException("User is blocked.");
    }
    String randomPassword = SecurityUtils.getRandomPassword(UserValidator.PASSWORD_MIN_LENGTH);
    String message = UserAccountsEmailMessages.buildTempResetMessage(randomPassword);
    emailBean.sendEmail(user.getEmail(), Message.RecipientType.TO, UserAccountsEmailMessages.ACCOUNT_PASSWORD_RESET,
        message);
    changePassword(user, randomPassword, req);
    resetFalseLogin(user);
    accountAuditFacade.registerAccountChange(user, AccountsAuditActions.RECOVERY.name(), UserAuditActions.SUCCESS.
        name(), "Password reset.", user, req);
  }

  /**
   * Test if two factor is enabled in the system and by the user
   *
   * @param user
   * @return
   */
  public boolean isTwoFactorEnabled(Users user) {
    String twoFactorAuth = settings.getTwoFactorAuth();
    String twoFactorExclude = settings.getTwoFactorExclude();
    String twoFactorMode = (twoFactorAuth != null ? twoFactorAuth : "");
    String excludes = (twoFactorExclude != null ? twoFactorExclude : null);
    String[] groups = (excludes != null && !excludes.isEmpty() ? excludes.split(";") : new String[]{});

    for (String group : groups) {
      if (isUserInRole(user, group)) {
        return false; //will allow anyone if one of the users groups are in the exclude list
      }
    }
    if (twoFactorMode.equals(Settings.TwoFactorMode.MANDATORY.getName())) {
      return true;
    } else if (twoFactorMode.equals(Settings.TwoFactorMode.OPTIONAL.getName()) && user.getTwoFactor()) {
      return true;
    }

    return false;
  }

  /**
   * Test if two factor is enabled
   *
   * @return
   */
  public boolean isTwoFactorEnabled() {
    String twoFactorAuth = settings.getTwoFactorAuth();
    String twoFactorMode = (twoFactorAuth != null ? twoFactorAuth : "");
    return twoFactorMode.equals(Settings.TwoFactorMode.MANDATORY.getName()) || twoFactorMode.equals(
        Settings.TwoFactorMode.OPTIONAL.getName());
  }

  public String getPasswordHash(String password, String salt) {
    return DigestUtils.sha256Hex(getPasswordPlusSalt(password, salt));
  }

  public String getHash(String val) {
    return DigestUtils.sha256Hex(val);
  }

  /**
   * Change password to the given password. Will generate a new salt
   *
   * @param user
   * @param password
   * @param req
   * @throws Exception
   */
  public void changePassword(Users user, String password, HttpServletRequest req) throws Exception {
    String salt = generateSalt();
    String passwordWithSalt = getPasswordHash(password, salt);
    resetProjectCertPassword(user, passwordWithSalt);
    user.setPassword(passwordWithSalt);
    user.setSalt(salt);
    user.setPasswordChanged(new Timestamp(new Date().getTime()));
    userFacade.update(user);
    accountAuditFacade.registerAccountChange(user, AccountsAuditActions.SECQUESTION.name(),
        AccountsAuditActions.SUCCESS.name(), "Changed password.", user, req);
  }

  /**
   * Change security question and adds account audit for the operation.
   * @param user
   * @param securityQuestion
   * @param securityAnswer
   * @param req 
   */
  public void changeSecQA(Users user, String securityQuestion, String securityAnswer, HttpServletRequest req) {
    user.setSecurityQuestion(SecurityQuestion.getQuestion(securityQuestion));
    user.setSecurityAnswer(DigestUtils.sha256Hex(securityAnswer.toLowerCase()));
    userFacade.update(user);
    accountAuditFacade.registerAccountChange(user, AccountsAuditActions.SECQUESTION.name(),
        AccountsAuditActions.SUCCESS.name(), "Changed Security Question.", user, req);
  }

  private String getPasswordPlusSalt(String password, String salt) {
    return password + salt;
  }

  private void resetProjectCertPassword(Users p, String pass) throws Exception {
    //For every project, change the certificate secret in the database
    //Get cert password by decrypting it with old password

    List<Project> projects = projectFacade.findAllMemberStudies(p);
    //In case of failure, keep a list of old certs 
    List<UserCerts> oldCerts = userCertsFacade.findUserCertsByUid(p.getUsername());
    List<ProjectGenericUserCerts> pguCerts = null;
    try {
      for (Project project : projects) {
        UserCerts userCert = userCertsFacade.findUserCert(project.getName(), p.getUsername());
        String certPassword = HopsUtils.decrypt(p.getPassword(), userCert.getUserKeyPwd());
        //Encrypt it with new password and store it in the db
        String newSecret = HopsUtils.encrypt(pass, certPassword);
        userCert.setUserKeyPwd(newSecret);
        userCertsFacade.persist(userCert);

        //If user is owner of the project, update projectgenericuser certs as well
        if (project.getOwner().equals(p)) {
          if (pguCerts == null) {
            pguCerts = new ArrayList<>();
          }
          ProjectGenericUserCerts pguCert = userCertsFacade.findProjectGenericUserCerts(project.getName()
              + Settings.PROJECT_GENERIC_USER_SUFFIX);
          pguCerts.add(userCertsFacade.findProjectGenericUserCerts(project.getName()
              + Settings.PROJECT_GENERIC_USER_SUFFIX));
          String pguCertPassword = HopsUtils.decrypt(p.getPassword(), pguCert.getCertificatePassword());
          //Encrypt it with new password and store it in the db
          String newPguSecret = HopsUtils.encrypt(pass, pguCertPassword);
          pguCert.setCertificatePassword(newPguSecret);
          userCertsFacade.persistPGUCert(pguCert);
        }
      }
    } catch (Exception ex) {
      LOGGER.log(Level.SEVERE, null, ex);
      //Persist old certs
      for (UserCerts oldCert : oldCerts) {
        userCertsFacade.persist(oldCert);
      }
      if (pguCerts != null) {
        for (ProjectGenericUserCerts pguCert : pguCerts) {
          userCertsFacade.persistPGUCert(pguCert);
        }
      }
      throw new Exception(ex);
    }

  }

  /**
   * Register failed login attempt.
   * @param user
   * @param req
   * @throws MessagingException 
   */
  public void registerFalseLogin(Users user, HttpServletRequest req) throws MessagingException {
    if (user != null) {
      int count = user.getFalseLogin() + 1;
      user.setFalseLogin(count);

      // block the user account if more than allowed false logins
      if (count > AuthenticationConstants.ALLOWED_FALSE_LOGINS) {
        user.setStatus(PeopleAccountStatus.BLOCKED_ACCOUNT);
        emailBean.sendEmail(user.getEmail(), Message.RecipientType.TO,
            UserAccountsEmailMessages.ACCOUNT_BLOCKED__SUBJECT, UserAccountsEmailMessages.accountBlockedMessage());
        accountAuditFacade.registerRoleChange(user, PeopleAccountStatus.SPAM_ACCOUNT.name(), RolesAuditActions.SUCCESS.
            name(), "False login retries:" + Integer.toString(count), user, req);
      }
      // notify user about the false attempts
      userFacade.update(user);
    }
  }

  private void registerFalseLogin(Users user) throws MessagingException {
    if (user != null) {
      int count = user.getFalseLogin() + 1;
      user.setFalseLogin(count);

      // block the user account if more than allowed false logins
      if (count > AuthenticationConstants.ALLOWED_FALSE_LOGINS) {
        user.setStatus(PeopleAccountStatus.BLOCKED_ACCOUNT);
        emailBean.sendEmail(user.getEmail(), Message.RecipientType.TO,
            UserAccountsEmailMessages.ACCOUNT_BLOCKED__SUBJECT, UserAccountsEmailMessages.accountBlockedMessage());
      }
      // notify user about the false attempts
      userFacade.update(user);
    }
  }

  /**
   * Registers failed email validation
   * @param user
   * @param req 
   */
  public void registerFalseKeyValidation(Users user, HttpServletRequest req) {
    if (user != null) {
      int count = user.getFalseLogin() + 1;
      user.setFalseLogin(count);

      // make the user spam account if more than allowed tries
      if (count > AuthenticationConstants.ACCOUNT_VALIDATION_TRIES) {
        user.setStatus(PeopleAccountStatus.SPAM_ACCOUNT);
      }
      userFacade.update(user);
      accountAuditFacade.registerRoleChange(user, PeopleAccountStatus.SPAM_ACCOUNT.name(), RolesAuditActions.SUCCESS.
          name(), "Wrong validation key retries: " + Integer.toString(count), user, req);
    }
  }

  /**
   * Sets false login to 0 for the given user
   * @param user 
   */
  public void resetFalseLogin(Users user) {
    if (user != null) {
      user.setFalseLogin(0);
      userFacade.update(user);
    }
  }

  /**
   * Set the user as online
   * @param user
   * @param status 
   */
  public void setUserOnlineStatus(Users user, int status) {
    if (user != null) {
      user.setIsonline(status);
      userFacade.update(user);
    }
  }

  private boolean isUserInRole(Users user, String groupName) {
    if (user == null || groupName == null) {
      return false;
    }
    BbcGroup group = bbcGroupFacade.findByGroupName(groupName);
    if (group == null) {
      return false;
    }
    return user.getBbcGroupCollection().contains(group);
  }

  /**
   * Generates a salt value with SALT_LENGTH and DIGEST
   * @return 
   */
  public String generateSalt() {
    return "";
  }
}
