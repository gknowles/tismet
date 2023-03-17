/*
Copyright Glen Knowles 2023.
Distributed under the Boost Software License, Version 1.0.

initapp.js - tismet webapp
*/

//===========================================================================
function navTopIntro(selected) {
    addOpts({
        computed: {
            navTopSelected() { return selected },
        },
        methods: {
            navTop() {
                return [
                    { name: 'Admin', href: 'admin-about.html' },
                    { name: 'Graph' },
                    { name: 'Debug', href: 'srv/about-counters.html' },
                ]
            },
            sourceHost() { return "http://github.com/gknowles/tismet" },
        },
    })
    includeHtmlFragment('srv/navtop.html')
}
