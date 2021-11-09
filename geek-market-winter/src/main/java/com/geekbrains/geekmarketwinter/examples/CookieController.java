package com.geekbrains.geekmarketwinter.examples;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.Arrays;

@Controller
@RequestMapping("/cookie")
public class CookieController {
    @RequestMapping("/add")
    @ResponseBody
    public String addCookie(HttpServletResponse response) {
        Cookie helloCookie = new Cookie("data", "hello");
        response.addCookie(helloCookie);
        return "cookie added";
    }

    @RequestMapping("/check")
    @ResponseBody
    public String checkCookie(HttpServletRequest request) {
        Cookie[] cookies = request.getCookies();
        Cookie cookie = Arrays.stream(cookies).filter(c -> c.getName().equals("data")).findAny().orElse(new Cookie("empty", "empty"));
        return cookie.getName() + " " + cookie.getValue();
    }

    @RequestMapping("/remove")
    @ResponseBody
    public String removeCookie(HttpServletResponse response) {
        Cookie helloCookie = new Cookie("data", "hello");
        helloCookie.setMaxAge(0);
        response.addCookie(helloCookie);
        return "cookie removed";
    }
}
