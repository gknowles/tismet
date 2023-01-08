/*
Copyright Glen Knowles 2022.
Distributed under the Boost Software License, Version 1.0.

about-common.js - dim webapp
*/

//===========================================================================
function aboutIntro(selected) {
    addOpts({
        computed: {
            miniNavSelected() {
                return selected
            },
        },
        methods: {
            miniNav() {
                return [
                    { name: 'Counters', href: 'about-counters.html' },
                    { name: 'Account', href: 'about-account.html' },
                    { name: 'Computer', href: 'about-computer.html' },
                    { name: 'Memory', href: 'about-memory.html' },
                ]
            },
        },
    })
    includeHtmlFragment('../navbar.html')
    includeHtmlFragment('navbar-debug.html')
    includeHtmlFragment('about-common.html')
    document.currentScript.remove()
}
