package com.geekbrains.geekmarketwinter.services;

import com.geekbrains.geekmarketwinter.entites.Role;
import com.geekbrains.geekmarketwinter.entites.SystemUser;
import com.geekbrains.geekmarketwinter.entites.User;
import com.geekbrains.geekmarketwinter.repositories.RoleRepository;
import com.geekbrains.geekmarketwinter.repositories.UserRepository;
import com.geekbrains.geekmarketwinter.repositories.UserRepositorySlqO2;
import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.security.core.GrantedAuthority;
//import org.springframework.security.core.authority.SimpleGrantedAuthority;
//import org.springframework.security.core.userdetails.UserDetails;
//import org.springframework.security.core.userdetails.UsernameNotFoundException;
//import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Collectors;

@Service
public class UserServiceImpl implements UserService {
    private UserRepository userRepository;
    private RoleRepository roleRepository;
 //   private BCryptPasswordEncoder passwordEncoder;
//    private UserRepositorySlqO2 userProvider;

//    @Autowired
//    public void setUserProvider(UserRepositorySlqO2 userProvider) {
//        this.userProvider = userProvider;
//    }

    @Autowired
    public void setUserRepository(UserRepository userRepository) {
        this.userRepository = userRepository;
    }

    @Autowired
    public void setRoleRepository(RoleRepository roleRepository) {
        this.roleRepository = roleRepository;
    }

//    @Autowired
//    public void setPasswordEncoder(BCryptPasswordEncoder passwordEncoder) {
//        this.passwordEncoder = passwordEncoder;
//    }

    @Override
    @Transactional
    public User findByUserName(String username) {
        //return userProvider.getUser(username);
        return userRepository.findOneByUserName(username);
    }

    @Override
    @Transactional
    public boolean save(SystemUser systemUser) {
        User user = new User();

        if (findByUserName(systemUser.getUserName()) != null) {
            return false;
        }

        user.setUserName(systemUser.getUserName());
     //   user.setPassword(passwordEncoder.encode(systemUser.getPassword()));
        user.setFirstName(systemUser.getFirstName());
        user.setLastName(systemUser.getLastName());
        user.setEmail(systemUser.getEmail());

        user.setRoles(Arrays.asList(roleRepository.findOneByName("ROLE_EMPLOYEE")));

        userRepository.save(user);
        return true;
    }
//

//    @Override
//    @Transactional
//    public UserDetails loadUserByUsername(String userName) throws UsernameNotFoundException {
//        User user = userRepository.findOneByUserName(userName);
//        if (user == null) {
//            throw new UsernameNotFoundException("Invalid username or password.");
//        }
//        return new org.springframework.security.core.userdetails.User(user.getUserName(), user.getPassword(),
//                mapRolesToAuthorities(user.getRoles()));
//    }
//
//    private Collection<? extends GrantedAuthority> mapRolesToAuthorities(Collection<Role> roles) {
//        return roles.stream().map(role -> new SimpleGrantedAuthority(role.getName())).collect(Collectors.toList());
//    }
}